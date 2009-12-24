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
import java.lang.ref.ReferenceQueue;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;

import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

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

    private static ConcurrentLinkedHashMap<ColumnKey, IndexEntry> createKeyCache(int size)
    {
        return ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, size);
    }

    // offset of the first block in the data file
    FileDeletingReference phantomReference;
    private ConcurrentLinkedHashMap<ColumnKey, IndexEntry> keyCache;

    /**
     * Package protected: to open an SSTableReader, use the open(*) factory methods.
     */
    SSTableReader(String filename, IPartitioner partitioner, List<IndexEntry> indexEntries, BloomFilter bloomFilter, int keysToCache)
    {
        super(filename, partitioner);
        this.indexEntries = indexEntries;
        this.bf = bloomFilter;
        phantomReference = new FileDeletingReference(this, finalizerQueue);
        finalizers.add(phantomReference);
        openedFiles.put(filename, this);
        this.keyCache = keysToCache < 1 ? null : createKeyCache(keysToCache);
    }

    private SSTableReader(String filename, IPartitioner partitioner)
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
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        try
        {
            indexEntries.clear();

            int i = 0;
            long indexSize = input.length();
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                if (i++ % INDEX_INTERVAL == 0)
                    indexEntries.add(IndexEntry.deserialize(input));
                else
                    IndexEntry.skip(input);
            }
        }
        finally
        {
            input.close();
        }
    }

    /**
     * @return The last cached index entry less than or equal to the given target.
     * The entry contains the position to begin scanning the index file or the
     * data file.
     */
    private IndexEntry getIndexScanPosition(ColumnKey target)
    {
        assert !indexEntries.isEmpty();
        int index = Collections.binarySearch(indexEntries, target, comparator);
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return null;
            return indexEntries.get(greaterThan - 1);
        }
        else
        {
            // TODO: for exact matches, utilize the dataOffset to seek directly to
            // the data file
            return indexEntries.get(index);
        }
    }

    /**
     * @return The position of the block that might contain the target key,
     * or -1 if the key is not contained in this SSTable.
     */
    long getBlockPosition(ColumnKey target) throws IOException
    {
        if (!bf.isPresent(comparator.forBloom(target)))
            return -1;
        if (keyCache != null)
        {
            IndexEntry cachedEntry = keyCache.get(target);
            if (cachedEntry != null)
                return cachedEntry.dataOffset;
        }
        IndexEntry indexEntry = getIndexScanPosition(target);
        if (indexEntry == null)
        {
            return -1;
        }

        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(path), "r");
        input.seek(indexEntry.indexOffset);
        int i = 0;
        try
        {
            do
            {
                try
                {
                    indexEntry = IndexEntry.deserialize(input);
                }
                catch (EOFException e)
                {
                    return -1;
                }
                int v = comparator.compare(indexEntry, target);
                if (v == 0)
                {
                    if (keyCache != null)
                        keyCache.put(indexEntry, indexEntry);
                    return indexEntry.dataOffset;
                }
                // FIXME: keys will not always exist in the index now, so we need to
                // return the position pointed to by the last entry
                throw new RuntimeException("FIXME: Not implemented.");
            } while  (++i < INDEX_INTERVAL);
        }
        finally
        {
            input.close();
        }
        //return -1;
    }

    /**
     * Factory function for blocks within this SSTable.
     */
    public Block getBlock(BufferedRandomAccessFile file, long blockPosition)
    {
        // TODO: plug in an optional (decompressed) block cache, to trade memory
        // for cpu.
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
        new File(compactedFilename()).createNewFile();
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
    class Block
    {
        public final BufferedRandomAccessFile file;
        // offset from the beginning of the data file
        public final long offset;
        // the currently opened stream for this block, or null;
        public DataInputStream stream;

        /**
         * To construct a Block, use SSTReader.getBlock().
         */
        private Block(BufferedRandomAccessFile file, long offset)
        {
            this.file = file;
            this.offset = offset;
            stream = null;
        }

        /**
         * Resets the block: calling stream() after this method will start streaming
         * from the beginning of the block.
         * @return this.
         */
        public Block reset() throws IOException
        {
            stream = null;
            file.seek(offset);
            return this;
        }

        /**
         * Calling stream() on a newly created Block will begin streaming from the
         * beginning of the block. Calling stream() additional times will return the
         * same stream object until reset() is called.
         *
         * TODO: This stream is unbounded, so for an uncompressed block, a bad caller
         * could read past a BLOCK_END slicemark into the next block. For a compressed
         * stream, the compression format will prevent this.
         *
         * @return An InputStream appropriate for reading the content of this block
         * from disk.
         */
        public DataInputStream stream() throws IOException
        {
            if (stream != null)
                // return the existing stream
                return stream;

            // read the block header
            BlockHeader mark = BlockHeader.deserialize(file);

            // TODO: handle setting up an appropriate decompression stream here

            // FIXME: we need to implement streams that utilize the BRAF buffer.
            stream = new DataInputStream(new FileInputStream(file.getFD()));
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
            SSTable.delete(path);
        }
    }
}
