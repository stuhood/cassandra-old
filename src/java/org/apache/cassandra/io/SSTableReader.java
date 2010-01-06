/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;

import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.MappedFileDataInput;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable implements Comparable<SSTableReader>
{
    private static final Logger logger = Logger.getLogger(SSTableReader.class);

    private static final FileSSTableMap openedFiles = new FileSSTableMap();
    
    // `finalizers` is required to keep the PhantomReferences alive after the enclosing SSTR is itself
    // unreferenced.  otherwise they will never get enqueued.
    private static final Set<Reference<SSTableReader>> finalizers = new HashSet<Reference<SSTableReader>>();
    private static final ReferenceQueue<SSTableReader> finalizerQueue = new ReferenceQueue<SSTableReader>()
    {{
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    FileDeletingReference r = null;
                    try
                    {
                        r = (FileDeletingReference) finalizerQueue.remove();
                        finalizers.remove(r);
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                    try
                    {
                        r.cleanup();
                    }
                    catch (IOException e)
                    {
                        logger.error("Error deleting " + r.path, e);
                    }
                }
            }
        };
        new Thread(runnable, "SSTABLE-DELETER").start();
    }};
    private final int BUFFER_SIZE = Integer.MAX_VALUE;

    public static int indexInterval()
    {
        return INDEX_INTERVAL;
    }

    public static long getApproximateKeyCount()
    {
        return getApproximateKeyCount(openedFiles.values());
    }

    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = 0;

        for (SSTableReader sstable : sstables)
        {
            int indexKeyCount = sstable.getIndexEntries().size();
            count = count + (indexKeyCount + 1) * INDEX_INTERVAL;
            if (logger.isDebugEnabled())
                logger.debug("index size for bloom filter calc for file  : " + sstable.getFilename() + "   : " + count);
        }

        return count;
    }

    /**
     * Get sampled indexed keys defined by the two predicates.
     * @param cfpred A Predicate defining matching column families.
     * @param dkpred A Predicate defining matching DecoratedKeys.
     */
    public static List<DecoratedKey> getIndexedDecoratedKeysFor(Predicate<SSTable> cfpred, Predicate<DecoratedKey> dkpred)
    {
        List<DecoratedKey> indexedKeys = new ArrayList<DecoratedKey>();
        
        for (SSTableReader sstable : openedFiles.values())
        {
            if (!cfpred.apply(sstable))
                continue;
            for (IndexEntry ie : sstable.getIndexEntries())
            {
                if (dkpred.apply(ie.dk))
                {
                    indexedKeys.add(ie.dk);
                }
            }
        }
        Collections.sort(indexedKeys);

        return indexedKeys;
    }

    /**
     * Get sampled indexed keys in any SSTable for our primary range.
     */
    public static List<DecoratedKey> getIndexedDecoratedKeys()
    {
        final Range range = StorageService.instance().getLocalPrimaryRange();

        Predicate<SSTable> cfpred = Predicates.alwaysTrue();
        return getIndexedDecoratedKeysFor(cfpred,
                                          new Predicate<DecoratedKey>(){
            public boolean apply(DecoratedKey dk)
            {
               return range.contains(dk.token);
            }
        });
    }

    public static SSTableReader open(String dataFileName) throws IOException
    {
        return open(dataFileName, StorageService.getPartitioner(), DatabaseDescriptor.getKeysCachedFraction(parseTableName(dataFileName)));
    }

    public static SSTableReader open(String dataFileName, IPartitioner partitioner, double cacheFraction) throws IOException
    {
        assert partitioner != null;
        assert openedFiles.get(dataFileName) == null;

        long start = System.currentTimeMillis();
        SSTableReader sstable = new SSTableReader(dataFileName, partitioner);
        sstable.loadIndexFile();
        sstable.loadBloomFilter();
        if (cacheFraction > 0)
            sstable.keyCache = createKeyCache((int)((sstable.getIndexEntries().size() + 1) * INDEX_INTERVAL * cacheFraction));
        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for "  + dataFileName + ": " + (System.currentTimeMillis() - start) + " ms.");

        return sstable;
    }

    FileDeletingReference phantomReference;
    private final MappedByteBuffer indexBuffer;
    private final MappedByteBuffer[] buffers; // jvm can only map up to 2GB at a time


    private static ConcurrentLinkedHashMap<ColumnKey, IndexEntry> createKeyCache(int size)
    {
        return ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, size);
    }

    private ConcurrentLinkedHashMap<ColumnKey, IndexEntry> keyCache;

    /**
     * Package protected: to open an SSTableReader, use the open(*) factory methods.
     */
    SSTableReader(String filename, IPartitioner partitioner, List<IndexEntry> indexEntries, BloomFilter bloomFilter, int keysToCache) throws IOException
    {
        super(filename, partitioner);
        this.indexEntries = indexEntries;
        indexBuffer = mmap(indexFilename());
        /* MERGE-FIXME: mmap disabled for now
        if (DatabaseDescriptor.getDiskAccessMode() == DatabaseDescriptor.DiskAccessMode.mmap)
        {
            int bufferCount = 1 + (int) (new File(path).length() / BUFFER_SIZE);
            buffers = new MappedByteBuffer[bufferCount];
            long remaining = length();
            for (int i = 0; i < bufferCount; i++)
            {
                buffers[i] = mmap(path, i * BUFFER_SIZE, (int) Math.min(remaining, BUFFER_SIZE));
                remaining -= BUFFER_SIZE;
            }
        }
        else
        {
            assert DatabaseDescriptor.getDiskAccessMode() == DatabaseDescriptor.DiskAccessMode.standard;
            buffers = null;
        }
        */
        buffers = null;

        this.bf = bloomFilter;
        phantomReference = new FileDeletingReference(this, finalizerQueue);
        finalizers.add(phantomReference);
        openedFiles.put(filename, this);
        this.keyCache = keysToCache < 1 ? null : createKeyCache(keysToCache);
    }

    private static MappedByteBuffer mmap(String filename, int start, int size) throws IOException
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile(filename, "r");
        }
        catch (FileNotFoundException e)
        {
            throw new IOError(e);
        }

        if (size < 0)
        {
            if (raf.length() > Integer.MAX_VALUE)
                throw new UnsupportedOperationException("File " + filename + " is too large to map in its entirety");
            size = (int) raf.length();
        }
        try
        {
            return raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size);
        }
        finally
        {
            raf.close();
        }
    }

    private static MappedByteBuffer mmap(String filename) throws IOException
    {
        return mmap(filename, 0, -1);
    }

    private SSTableReader(String filename, IPartitioner partitioner) throws IOException
    {
        this(filename, partitioner, new ArrayList<IndexEntry>(), null, 0);
    }

    public List<IndexEntry> getIndexEntries()
    {
        return indexEntries;
    }

    void loadBloomFilter() throws IOException
    {
        DataInputStream stream = new DataInputStream(new FileInputStream(filterFilename()));
        try
        {
            bf = BloomFilter.serializer().deserialize(stream);
        }
        finally
        {
            stream.close();
        }
    }

    void loadIndexFile() throws IOException
    {
        indexEntries.clear();
        
        // FIXME: 669 will allow indexes longer than this buffer, which means
        // we will no longer be able to trust getFilePointer: we need to change
        // the contract for FileDataInput to include offset info
        FileDataInput input = new MappedFileDataInput(indexBuffer, indexFilename());
        int i = 0;
        long indexSize = input.length();
        while (true)
        {
            if (input.getFilePointer() == indexSize)
            {
                break;
            }
            IndexEntry entry = IndexEntry.deserialize(input);
            if (i++ % INDEX_INTERVAL == 0)
            {
                indexEntries.add(entry);
            }
        }
    }

    private long cacheAndReturn(ColumnKey key, IndexEntry entry)
    {
        if (entry == null)
            return -1;
        if (keyCache != null)
            keyCache.put(key, entry);
        return entry.dataOffset;
    }

    /**
     * @return The position of the block that might contain the target key,
     * or -1 if the key is not contained in this SSTable.
     */
    long getBlockPosition(ColumnKey target) throws IOException
    {
        // see if the filter or cache can confirm/deny
        if (!target.isPresentInBloom(bf))
            return -1;
        if (keyCache != null)
        {
            IndexEntry cachedEntry = keyCache.get(target);
            if (cachedEntry != null)
                return cachedEntry.dataOffset;
        }

        // check the index
        assert !indexEntries.isEmpty();
        int indexPos = Collections.binarySearch(indexEntries, target, comparator);
        if (indexPos >= 0)
            // exact index match: no need to read from the index file
            return indexEntries.get(indexPos).dataOffset;

        // found an index entry near the one we want: first index _greater_ than the
        // key searched for, i.e., its insertion position
        int greaterThan = -(indexPos + 1);
        if (greaterThan == 0)
            // key would be contained before the first block in the file
            return -1;
        IndexEntry indexEntry = indexEntries.get(greaterThan - 1);

        // scan the index file from the entry we found
        FileDataInput input = new MappedFileDataInput(indexBuffer, indexFilename());
        input.seek(indexEntry.indexOffset);
        int i = 0;
        IndexEntry previous = null;
        try
        {
            do
            {
                indexEntry = IndexEntry.deserialize(input);
                // the block containing the key is the last block less than or
                // equal to the key
                int v = comparator.compare(indexEntry, target);
                if (v == 0)
                    return cacheAndReturn(target, indexEntry);
                else if (v > 0)
                    // previous block was the last less than the key
                    return cacheAndReturn(target, previous);
                // else, continue
                previous = indexEntry;
            } while  (++i < INDEX_INTERVAL);
        }
        catch (EOFException e)
        {
            // pass
        }
        finally
        {
            input.close();
        }
        return cacheAndReturn(target, previous);
    }

    /**
     * @return The position of the first block with a key greater than or equal
     * to the target key, or -1 if such a block is not contained in this SSTable.
     */
    long nearestBlockPosition(ColumnKey target) throws IOException
    {
        // see if the exact location of the key is cached
        if (keyCache != null)
        {
            IndexEntry cachedEntry = keyCache.get(target);
            if (cachedEntry != null)
                return cachedEntry.dataOffset;
        }

        // check the index
        assert !indexEntries.isEmpty();
        int indexPos = Collections.binarySearch(indexEntries, target, comparator);
        if (indexPos >= 0)
            // exact index match: no need to read from the index file
            return indexEntries.get(indexPos).dataOffset;

        // found an index entry near the one we want: first index _greater_ than the
        // key searched for, i.e., its insertion position
        int greaterThan = -(indexPos + 1);
        if (greaterThan == indexEntries.size())
            // key might be contained in the final blocks of the file
            greaterThan = greaterThan - 1;
        IndexEntry indexEntry = indexEntries.get(greaterThan);

        // scan the index file from the entry we found
        FileDataInput input = new MappedFileDataInput(indexBuffer, indexFilename());
        input.seek(indexEntry.indexOffset);
        int i = 0;
        IndexEntry previous = indexEntry;
        try
        {
            do
            {
                indexEntry = IndexEntry.deserialize(input);
                int v = comparator.compare(indexEntry, target);
                if (v == 0)
                    // exact matches can be cached
                    return cacheAndReturn(target, indexEntry);
                else if (v > 0)
                    break;
                // else, continue
                previous = indexEntry;
            } while  (++i < INDEX_INTERVAL);
        }
        catch (EOFException e)
        {
            // pass
        }
        finally
        {
            input.close();
        }
        // previous block was the first that might contain content >= to the key
        return previous.dataOffset;
    }

    /**
     * Factory function for blocks within this SSTable.
     */
    public Block getBlock(BufferedRandomAccessFile file, long blockPosition)
    {
        return new Block(file, blockPosition);
    }

    public long length()
    {
        return new File(path).length();
    }

    public int compareTo(SSTableReader o)
    {
        return ColumnFamilyStore.getGenerationFromFileName(path) - ColumnFamilyStore.getGenerationFromFileName(o.path);
    }

    public void markCompacted() throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + path + " compacted");
        openedFiles.remove(path);
        if (!new File(compactedFilename()).createNewFile())
        {
            throw new IOException("Unable to create compaction marker");
        }
        phantomReference.deleteOnCleanup();
    }

    /** obviously only for testing */
    public void forceBloomFilterFailures()
    {
        bf = BloomFilter.alwaysMatchingBloomFilter();
    }

    static void reopenUnsafe() throws IOException // testing only
    {
        Collection<SSTableReader> sstables = new ArrayList<SSTableReader>(openedFiles.values());
        openedFiles.clear();
        for (SSTableReader sstable : sstables)
        {
            SSTableReader.open(sstable.path, sstable.partitioner, 0.01);
        }
    }

    public IPartitioner getPartitioner()
    {
        return partitioner;
    }

    /**
     * @return A Scanner for this SSTable with given disk buffer size.
     */
    public SSTableScanner getScanner(int bufferSize) throws IOException
    {
        try
        {
            return new SSTableScanner(this, bufferSize);
        }
        catch (IOException e)
        {
            throw new IOException("Could not open scanner for " + path, e);
        }
    }

    /**
     * MERGE-FIXME: all use of this method is likely to be broken: FileDataInput needs to be plugged into the scanner
     */
    /*
    public FileDataInput getFileDataInput(DecoratedKey decoratedKey, int bufferSize) throws IOException
    {
        PositionSize info = getPosition(decoratedKey);
        if (info == null)
            return null;

        if (buffers == null || (bufferIndex(info.position) != bufferIndex(info.position + info.size)))
        {
            BufferedRandomAccessFile file = new BufferedRandomAccessFile(path, "r", bufferSize);
            file.seek(info.position);
            return file;
        }
        return new MappedFileDataInput(buffers[bufferIndex(info.position)], path, (int) (info.position % BUFFER_SIZE));
    }

    private int bufferIndex(long position)
    {
        return (int) (position / BUFFER_SIZE);
    }
    */

    /**
     * Deprecated: should remove in favor of the ColumnKey.Comparator every SSTable
     * holds.
     */
    @Deprecated
    public AbstractType getColumnComparator()
    {
        return DatabaseDescriptor.getComparator(getTableName(), getColumnFamilyName());
    }

    public ColumnFamily makeColumnFamily()
    {
        return ColumnFamily.create(getTableName(), getColumnFamilyName());
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        return DatabaseDescriptor.getColumnFamilyType(getTableName(), getColumnFamilyName()).equals("Standard")
               ? Column.serializer()
               : SuperColumn.serializer(getColumnComparator());
    }

    /**
     * A block in the SSTable, with a method to open a stream for the block.
     *
     * As a non-static class, a Block holds a reference to the SSTable it was
     * created for and should prevent it from being cleaned up.
     */
    final class Block
    {
        public final BufferedRandomAccessFile file;
        // offset from the beginning of the data file to the block header
        public final long offset;

        // the currently opened stream for this block, or null;
        private DataInputStream stream = null;
        // block header, or null: lazily created for stream() or header()
        private BlockHeader header = null;

        /**
         * To construct a Block, use SSTReader.getBlock().
         */
        private Block(BufferedRandomAccessFile file, long offset)
        {
            this.file = file;
            this.offset = offset;
        }

        private void readHeader() throws IOException
        {
            file.seek(offset);
            header = BlockHeader.deserialize(file);
            assert (file.length() - file.getFilePointer()) <= header.blockLen :
                "Block crosses file or buffer boundaries.";
        }

        public BlockHeader header() throws IOException
        {
            if (header != null)
                return header;
            readHeader();
            return header;
        }

        /**
         * Resets the block: calling stream() after this method will start streaming
         * from the beginning of the block.
         * @return this.
         */
        public Block reset() throws IOException
        {
            stream = null;
            return this;
        }

        /**
         * Calling stream() on a newly created Block will begin streaming from the
         * beginning of the block. Calling stream() additional times will return the
         * same stream object until reset() is called.
         *
         * @return An InputStream appropriate for reading the content of this block
         * from disk.
         */
        public DataInputStream stream() throws IOException
        {
            if (stream != null)
                // return the existing stream
                return stream;

            readHeader();

            // TODO: handle setting up an appropriate decompression stream here
            stream = new DataInputStream(new BoundedInputStream(file.inputStream(), header.blockLen));
            return stream;
        }
    }
}

class FileSSTableMap
{
    private final Map<String, SSTableReader> map = new NonBlockingHashMap<String, SSTableReader>();

    public SSTableReader get(String filename)
    {
        try
        {
            return map.get(new File(filename).getCanonicalPath());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public SSTableReader put(String filename, SSTableReader value)
    {
        try
        {
            return map.put(new File(filename).getCanonicalPath(), value);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Collection<SSTableReader> values()
    {
        return map.values();
    }

    public void clear()
    {
        map.clear();
    }

    public void remove(String filename) throws IOException
    {
        map.remove(new File(filename).getCanonicalPath());
    }

    @Override
    public String toString()
    {
        return "FileSSTableMap {" + StringUtils.join(map.keySet(), ", ") + "}";
    }
}

class FileDeletingReference extends PhantomReference<SSTableReader>
{
    public final String path;
    private boolean deleteOnCleanup;

    FileDeletingReference(SSTableReader referent, ReferenceQueue<? super SSTableReader> q)
    {
        super(referent, q);
        this.path = referent.path;
    }

    public void deleteOnCleanup()
    {
        deleteOnCleanup = true;
    }

    public void cleanup() throws IOException
    {
        if (deleteOnCleanup)
        {
            // this is tricky because the mmapping might not have been finalized yet,
            // and delete will until it is.  additionally, we need to make sure to
            // delete the data file first, so on restart the others will be recognized as GCable
            // even if the compaction file deletion occurs next.
            new Thread(new Runnable()
            {
                public void run()
                {
                    File datafile = new File(path);
                    for (int i = 0; i < DeletionService.MAX_RETRIES; i++)
                    {
                        if (datafile.delete())
                            break;
                        try
                        {
                            Thread.sleep(10000);
                        }
                        catch (InterruptedException e)
                        {
                            throw new AssertionError(e);
                        }
                    }
                    if (datafile.exists())
                        throw new RuntimeException("Unable to delete " + path);
                    SSTable.logger.info("Deleted " + path);
                    DeletionService.submitDeleteWithRetry(SSTable.indexFilename(path));
                    DeletionService.submitDeleteWithRetry(SSTable.filterFilename(path));
                    DeletionService.submitDeleteWithRetry(SSTable.compactedFilename(path));
                }
            }).start();
        }
    }
}
