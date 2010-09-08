/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.io.sstable;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.*;


import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.sstable.bitidx.BitmapIndexReader;
import org.apache.cassandra.io.sstable.bitidx.OpenSegment;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Collections2;

import org.apache.cassandra.thrift.IndexExpression;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public class SSTableReader extends SSTable implements Comparable<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    // guesstimated size of INDEX_INTERVAL index entries
    private static final int INDEX_FILE_BUFFER_BYTES = 16 * DatabaseDescriptor.getIndexInterval();

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
                    SSTableDeletingReference r;
                    try
                    {
                        r = (SSTableDeletingReference) finalizerQueue.remove();
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
                        logger.error("Error deleting " + r.desc, e);
                    }
                }
            }
        };
        new Thread(runnable, "SSTABLE-DELETER").start();
    }};

    /**
     * maxDataAge is a timestamp in local server time (e.g. System.currentTimeMilli) which represents an uppper bound
     * to the newest piece of data stored in the sstable. In other words, this sstable does not contain items created
     * later than maxDataAge.
     * 
     * The field is not serialized to disk, so relying on it for more than what truncate does is not advised.
     *
     * When a new sstable is flushed, maxDataAge is set to the time of creation.
     * When a sstable is created from compaction, maxDataAge is set to max of all merged tables.
     *
     * The age is in milliseconds since epoc and is local to this host.
     */
    public final long maxDataAge;

    // indexfile and datafile: might be null before a call to load()
    private SegmentedFile ifile;
    private SegmentedFile dfile;

    private IndexSummary indexSummary;
    private BloomFilter bf;
    private Map<byte[],BitmapIndexReader> secindexes;

    private InstrumentedCache<Pair<Descriptor,DecoratedKey>, Long> keyCache;

    private BloomFilterTracker bloomFilterTracker = new BloomFilterTracker();

    private volatile SSTableDeletingReference phantomReference;

    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = 0;

        for (SSTableReader sstable : sstables)
        {
            int indexKeyCount = sstable.getKeySamples().size();
            count = count + (indexKeyCount + 1) * DatabaseDescriptor.getIndexInterval();
            if (logger.isDebugEnabled())
                logger.debug("index size for bloom filter calc for file  : " + sstable.getFilename() + "   : " + count);
        }

        return count;
    }

    private void loadStatistics() throws IOException
    {
        File file = new File(descriptor.filenameFor(SSTable.COMPONENT_STATS));
        if (!file.exists())
        {
            estimatedRowSize = SSTable.defaultRowHistogram();
            estimatedColumnCount = SSTable.defaultColumnHistogram();
            return;
        }
        if (logger.isDebugEnabled())
            logger.debug("Load statistics for " + descriptor);
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        estimatedRowSize = EstimatedHistogram.serializer.deserialize(dis);
        estimatedColumnCount = EstimatedHistogram.serializer.deserialize(dis);
        dis.close();
    }

    public static SSTableReader open(Descriptor desc) throws IOException
    {
        Set<Component> components = SSTable.componentsFor(desc);
        return open(desc, components, DatabaseDescriptor.getCFMetaData(desc.ksname, desc.cfname), StorageService.getPartitioner());
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        return open(descriptor, components, Collections.<DecoratedKey>emptySet(), null, metadata, partitioner);
    }

    public static SSTableReader open(Descriptor descriptor, Set<Component> components, Set<DecoratedKey> savedKeys, SSTableTracker tracker, CFMetaData metadata, IPartitioner partitioner) throws IOException
    {
        assert partitioner != null;

        long start = System.currentTimeMillis();
        logger.info("Opening " + descriptor);

        SSTableReader sstable = new SSTableReader(descriptor, components, metadata, partitioner, null, null, null, null, System.currentTimeMillis(), null, null, null);
        sstable.setTrackedBy(tracker);

        // versions before 'c' encoded keys as utf-16 before hashing to the filter
        if (descriptor.hasStringsInBloomFilter)
        {
            sstable.load(true, savedKeys);
        }
        else
        {
            sstable.load(false, savedKeys);
            sstable.loadBloomFilter();
        }
        sstable.loadStatistics();
        sstable.loadSecondaryIndexes();

        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for " + descriptor + ": " + (System.currentTimeMillis() - start) + " ms.");

        if (logger.isDebugEnabled() && sstable.getKeyCache() != null)
            logger.debug(String.format("key cache contains %s/%s keys", sstable.getKeyCache().getSize(), sstable.getKeyCache().getCapacity()));

        return sstable;
    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    static SSTableReader internalOpen(Descriptor desc, Set<Component> components, CFMetaData metadata, IPartitioner partitioner, SegmentedFile ifile, SegmentedFile dfile, IndexSummary isummary, BloomFilter bf, long maxDataAge, EstimatedHistogram rowsize, EstimatedHistogram columncount, Map<byte[],BitmapIndexReader> secindexes) throws IOException
    {
        assert desc != null && partitioner != null && ifile != null && dfile != null && isummary != null && bf != null;
        assert rowsize != null && columncount != null && secindexes != null;
        return new SSTableReader(desc, components, metadata, partitioner, ifile, dfile, isummary, bf, maxDataAge, rowsize, columncount, secindexes);
    }

    private SSTableReader(Descriptor desc,
                          Set<Component> components,
                          CFMetaData metadata,
                          IPartitioner partitioner,
                          SegmentedFile ifile,
                          SegmentedFile dfile,
                          IndexSummary indexSummary,
                          BloomFilter bloomFilter,
                          long maxDataAge,
                          EstimatedHistogram rowSizes,
                          EstimatedHistogram columnCounts,
                          Map<byte[],BitmapIndexReader> secindexes)
    throws IOException
    {
        super(desc, components, metadata, partitioner, rowSizes, columnCounts);
        this.maxDataAge = maxDataAge;

        this.ifile = ifile;
        this.dfile = dfile;
        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
        estimatedRowSize = rowSizes;
        estimatedColumnCount = columnCounts;
        this.secindexes = secindexes;
    }

    public void setTrackedBy(SSTableTracker tracker)
    {
        if (tracker != null)
        {
            phantomReference = new SSTableDeletingReference(tracker, this, finalizerQueue);
            finalizers.add(phantomReference);
            keyCache = tracker.getKeyCache();
        }
    }

    private void loadSecondaryIndexes() throws IOException
    {
        secindexes = new TreeMap<byte[],BitmapIndexReader>(metadata.comparator);
        for (Component component : components)
        {
            if (component.type != Component.Type.BITMAP_INDEX)
                continue;
            
            BitmapIndexReader secindex = BitmapIndexReader.open(descriptor, component);
            secindexes.put(secindex.name(), secindex);
        }
    }

    private void loadBloomFilter() throws IOException
    {
        DataInputStream stream = new DataInputStream(new FileInputStream(descriptor.filenameFor(Component.FILTER)));
        try
        {
            bf = BloomFilter.serializer().deserialize(stream);
        }
        finally
        {
            stream.close();
        }
    }

    /**
     * Loads ifile, dfile and indexSummary, and optionally recreates the bloom filter.
     */
    private void load(boolean recreatebloom, Set<DecoratedKey> keysToLoadInCache) throws IOException
    {
        boolean cacheLoading = keyCache != null && !keysToLoadInCache.isEmpty();
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
        SegmentedFile.Builder dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(descriptor.filenameFor(Component.PRIMARY_INDEX), "r");
        try
        {
            if (keyCache != null && keyCache.getCapacity() - keyCache.getSize() < keysToLoadInCache.size())
                keyCache.updateCapacity(keyCache.getSize() + keysToLoadInCache.size());

            long indexSize = input.length();
            long estimatedKeys = SSTable.estimateRowsFromIndex(input);
            indexSummary = new IndexSummary(estimatedKeys);
            if (recreatebloom)
                // estimate key count based on index length
                bf = BloomFilter.getFilter(estimatedKeys, 15);
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                boolean shouldAddEntry = indexSummary.shouldAddEntry();
                ByteBuffer key = (ByteBuffer) ((shouldAddEntry || cacheLoading || recreatebloom)
                             ? FBUtilities.readShortByteArray(input)
                             : FBUtilities.skipShortByteArray(input));
                long dataPosition = input.readLong();
                if (key != null)
                {
                    DecoratedKey decoratedKey = decodeKey(partitioner, descriptor, key);
                    if (recreatebloom)
                        bf.add(decoratedKey.key);
                    if (shouldAddEntry)
                        indexSummary.addEntry(decoratedKey, indexPosition);
                    if (cacheLoading && keysToLoadInCache.contains(decoratedKey))
                        keyCache.put(new Pair(descriptor, decoratedKey), dataPosition);
                }

                indexSummary.incrementRowid();
                ibuilder.addPotentialBoundary(indexPosition);
                dbuilder.addPotentialBoundary(dataPosition);
            }
            indexSummary.complete();
        }
        finally
        {
            input.close();
        }

        // finalize the state of the reader
        ifile = ibuilder.complete(descriptor.filenameFor(Component.PRIMARY_INDEX));
        dfile = dbuilder.complete(descriptor.filenameFor(Component.DATA));
    }

    /**
     * For testing purposes only.
     */
    public void forceFilterFailures()
    {
        bf = BloomFilter.alwaysMatchingBloomFilter();
    }

    public BloomFilter getBloomFilter()
    {
      return bf;
    }

    /**
     * @return The key cache: for monitoring purposes.
     */
    public InstrumentedCache getKeyCache()
    {
        return keyCache;
    }

    /**
     * @return An estimate of the number of keys in this SSTable.
     */
    public long estimatedKeys()
    {
        return indexSummary.getIndexPositions().size() * DatabaseDescriptor.getIndexInterval();
    }

    /**
     * @return Approximately 1/INDEX_INTERVALth of the keys in this SSTable.
     */
    public Collection<DecoratedKey> getKeySamples()
    {
        return Collections2.transform(indexSummary.getIndexPositions(),
                                      new Function<IndexSummary.KeyPosition, DecoratedKey>(){
                                          public DecoratedKey apply(IndexSummary.KeyPosition kp)
                                          {
                                              return kp.key;
                                          }
                                      });
    }

    /**
     * Determine the minimal set of sections that can be extracted from this SSTable to cover the given ranges.
     * @return A sorted list of (offset,end) pairs that cover the given ranges in the datafile for this SSTable.
     */
    public List<Pair<Long,Long>> getPositionsForRanges(Collection<Range> ranges)
    {
        // use the index to determine a minimal section for each range
        List<Pair<Long,Long>> positions = new ArrayList<Pair<Long,Long>>();
        for (AbstractBounds range : AbstractBounds.normalize(ranges))
        {
            long left = getPosition(new DecoratedKey(range.left, null), Operator.GT);
            if (left == -1)
                // left is past the end of the file
                continue;
            long right = getPosition(new DecoratedKey(range.right, null), Operator.GT);
            if (right == -1 || Range.isWrapAround(range.left, range.right))
                // right is past the end of the file, or it wraps
                right = length();
            if (left == right)
                // empty range
                continue;
            positions.add(new Pair(Long.valueOf(left), Long.valueOf(right)));
        }
        return positions;
    }

    /**
     * @param decoratedKey The key to apply as the rhs to the given Operator.
     * @param op The Operator defining matching keys: the nearest key to the target matching the operator wins.
     * @return The position in the data file to find the key, or -1 if the key is not present
     */
    public long getPosition(DecoratedKey decoratedKey, Operator op)
    {
        // first, check bloom filter
        if (op == Operator.EQ && !bf.isPresent(decoratedKey.key))
            return -1;

        // next, the key cache
        Pair<Descriptor, DecoratedKey> unifiedKey = new Pair<Descriptor, DecoratedKey>(descriptor, decoratedKey);
        if (keyCache != null && keyCache.getCapacity() > 0)
        {
            Long cachedPosition = keyCache.get(unifiedKey);
            if (cachedPosition != null)
            {
                return cachedPosition;
            }
        }

        // next, see if the sampled index says it's impossible for the key to be present
        IndexSummary.KeyPosition sampledPosition = indexSummary.getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            if (op == Operator.EQ)
                bloomFilterTracker.addFalsePositive();
            // we matched the -1th position: if the operator might match forward, return the 0th position
            return op.apply(1) >= 0 ? 0 : -1;
        }

        // scan the on-disk index, starting at the nearest sampled position
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition.indexPosition, INDEX_FILE_BUFFER_BYTES);
        while (segments.hasNext())
        {
            FileDataInput input = segments.next();
            try
            {
                while (!input.isEOF())
                {
                    // read key & data position from index entry
                    DecoratedKey indexDecoratedKey = decodeKey(partitioner, descriptor, FBUtilities.readShortByteArray(input));
                    long dataPosition = input.readLong();

                    int comparison = indexDecoratedKey.compareTo(decoratedKey);
                    int v = op.apply(comparison);
                    if (v == 0)
                    {
                        if (comparison == 0 && keyCache != null && keyCache.getCapacity() > 0)
                        {
                            if (op == Operator.EQ)
                                bloomFilterTracker.addTruePositive();
                            // store exact match for the key
                            keyCache.put(unifiedKey, Long.valueOf(dataPosition));
                        }
                        return dataPosition;
                    }
                    if (v < 0)
                    {
                        if (op == Operator.EQ)
                            bloomFilterTracker.addFalsePositive();
                        return -1;
                    }
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    logger.error("error closing file", e);
                }
            }
        }

        if (op == Operator.EQ)
            bloomFilterTracker.addFalsePositive();
        return -1;
    }

    /**
     * @return An ordered iterator over keys that _probably_ match the given parameters. The
     * frequency of false positives depends on the quality of bin selection, and the cardinality
     * of the indexed fields. But since an SSTable is only a patch applied to previous points in
     * time, the keys returned by this method need to be resolved against keys returned by other
     * SSTables anyway.
     */
    public CloseableIterator<DecoratedKey> scan(IndexExpression expr, AbstractBounds range)
    {
        // use the primary index to find the bounds of the scan, in terms of rowids
        final Pair<Long,Long> rowidrange = indexSummary.getRowidRange(range);
        // TODO: if more than one useful expression, open a BMI for each, and squash
        // into a CompoundBMI: they should be cheap
        BitmapIndexReader reader = secindexes.get(expr.column_name);

        // create an iterator for matching rowids in the range
        final CloseableIterator<OpenSegment> rowids = reader.iterator(expr.op, expr.value, rowidrange);

        // return an iterator that looks up output rowids in the primary index
        return new PrimaryToSecondary(rowidrange, rowids);
    }

    /**
     * @return The length in bytes of the data file for this SSTable.
     */
    public long length()
    {
        return dfile.length;
    }

    public void markCompacted()
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + getFilename() + " compacted");
        try
        {
            if (!new File(descriptor.filenameFor(Component.COMPACTED_MARKER)).createNewFile())
                throw new IOException("Unable to create compaction marker");
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        phantomReference.deleteOnCleanup();
    }

    /**
     * @param bufferSize Buffer size in bytes for this Scanner.
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public SSTableScanner getScanner(int bufferSize)
    {
        return new SSTableScanner(this, bufferSize);
    }

    /**
     * @param bufferSize Buffer size in bytes for this Scanner.
     * @param filter filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public SSTableScanner getScanner(int bufferSize, QueryFilter filter)
    {
        return new SSTableScanner(this, filter, bufferSize);
    }

    public FileDataInput getFileDataInput(DecoratedKey decoratedKey, int bufferSize)
    {
        long position = getPosition(decoratedKey, Operator.EQ);
        if (position < 0)
            return null;

        return dfile.getSegment(position, bufferSize);
    }

    public int compareTo(SSTableReader o)
    {
        return descriptor.generation - o.descriptor.generation;
    }

    public AbstractType getColumnComparator()
    {
        return metadata.comparator;
    }

    public ColumnFamily createColumnFamily()
    {
        return ColumnFamily.create(metadata);
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        return metadata.cfType == ColumnFamilyType.Standard
               ? Column.serializer()
               : SuperColumn.serializer(getColumnComparator());
    }

    /**
     * Tests if the sstable contains data newer than the given age param (in localhost currentMilli time).
     * This works in conjunction with maxDataAge which is an upper bound on the create of data in this sstable.
     * @param age The age to compare the maxDataAre of this sstable. Measured in millisec since epoc on this host
     * @return True iff this sstable contains data that's newer than the given age parameter.
     */
    public boolean newSince(long age)
    {
        return maxDataAge > age;
    }

    public static long readRowSize(DataInput in, Descriptor d) throws IOException
    {
        if (d.hasIntRowSize)
            return in.readInt();
        return in.readLong();
    }

    public void createLinks(String snapshotDirectoryPath) throws IOException
    {
        for (Component component : components)
        {
            File sourceFile = new File(descriptor.filenameFor(component));
            File targetLink = new File(snapshotDirectoryPath, sourceFile.getName());
            CLibrary.createHardLink(sourceFile, targetLink);
        }
    }

    /**
     * Conditionally use the deprecated 'IPartitioner.convertFromDiskFormat' method.
     */
    public static DecoratedKey decodeKey(IPartitioner p, Descriptor d, ByteBuffer bytes)
    {
        if (d.hasEncodedKeys)
            return p.convertFromDiskFormat(bytes);
        return p.decorateKey(bytes);
    }

    public abstract static class Operator
    {
        public static final Operator EQ = new Equals();
        public static final Operator GE = new GreaterThanOrEqualTo();
        public static final Operator GT = new GreaterThan();

        /**
         * @param comparison The result of a call to compare/compareTo, with the desired field on the rhs.
         * @return less than 0 if the operator cannot match forward, 0 if it matches, greater than 0 if it might match forward.
         */
        public abstract int apply(int comparison);

        final static class Equals extends Operator
        {
            public int apply(int comparison) { return -comparison; }
        }

        final static class GreaterThanOrEqualTo extends Operator
        {
            public int apply(int comparison) { return comparison >= 0 ? 0 : -comparison; }
        }

        final static class GreaterThan extends Operator
        {
            public int apply(int comparison) { return comparison > 0 ? 0 : 1; }
        }
    }

    /**
     * An iterator that merge joins a bitmap secondary index (represented by an iterator of
     * OpenSegments) to the primary index.
     */
    final class PrimaryToSecondary extends AbstractIterator<DecoratedKey> implements CloseableIterator<DecoratedKey>
    {
        private final Pair<Long,Long> rowidrange;
        private Iterator<FileDataInput> primary;
        private FileDataInput pinput;
        // our absolute rowid position in the primary index on disk
        private long rowidposition;

        private final CloseableIterator<OpenSegment> secondary;
        private OpenSegment segment;
        // relative offset in the segment, and interesting ranges calculated from the absolute rowids
        private int localid;
        private int localidmin;
        private int localidmax;

        public PrimaryToSecondary(Pair<Long,Long> rowidrange, CloseableIterator<OpenSegment> secondary)
        {
            this.rowidrange = rowidrange;
            this.secondary = secondary;
            // the first lookup to the primary index must seek
            rowidposition = Long.MIN_VALUE;
        }

        public DecoratedKey computeNext()
        {
            // determine the next rowid in the secondary index
            secondary_scan: while (true)
            {
                if (segment == null)
                {
                    if (!secondary.hasNext())
                        // secondary index is exhausted
                        return endOfData();
                    segment = secondary.next();
                    localid = -1;
                    // the range of bits in this segment that might represent matches
                    localidmin = (int)(rowidrange.left - segment.rowid);
                    localidmax = (int)Math.min(rowidrange.right - segment.rowid, segment.numrows - 1);
                }

                // find the next matching rowid
                while ((localid = segment.bitset.nextSetBit(++localid)) != -1)
                {
                    if (localid > localidmax)
                        // exhausted this segment: break inner to try next segment
                        break; // inner
                    if (localid < localidmin)
                        // current row is before the first valid row in this segment
                        continue;
                    // matched a valid row in the segment
                    break secondary_scan;
                }
                // segment was exhausted
                segment = null;
            }

            // lookup the rowid in the primary index
            long rowid = localid + segment.rowid;
            if (rowidposition + DatabaseDescriptor.getIndexInterval() < rowid)
            {
                // the matched row is at least INDEX_INTERVAL rows away...
                if (pinput != null)
                    closePrimary(true);
                // seek on the primary index
                Pair<IndexSummary.KeyPosition,Long> position = indexSummary.getIndexScanPosition(rowid);
                primary = ifile.iterator(position.left.indexPosition, INDEX_FILE_BUFFER_BYTES);
                // our new physical position
                rowidposition = position.right;
                pinput = primary.next();
            }

            // scan the primary index
            while (true)
            {
                try
                {
                    while (!pinput.isEOF())
                    {
                        // count up to the interesting row 
                        if (rowidposition++ == rowid)
                        {
                            DecoratedKey dk = decodeKey(partitioner, descriptor, FBUtilities.readShortByteArray(pinput));
                            pinput.readLong();
                            return dk;
                        }
                        FBUtilities.readShortByteArray(pinput);
                        pinput.readLong();
                    }
                    // exhausted an input from the primary: the rowid must exist, so we don't bounds check
                    pinput.close();
                    pinput = primary.next();
                }
                catch (IOException e)
                {
                    closePrimary(false);
                    throw new IOError(e);
                }
            }
        }

        private void closePrimary(boolean shouldThrow) throws IOError
        {
            try
            {
                pinput.close();
            }
            catch (IOException e)
            {
                if (shouldThrow)
                    throw new IOError(e);
                else
                    logger.error("error closing file", e);
            }
        }

        public void close() throws IOError
        {
            secondary.close();
            closePrimary(true);
        }
    }

    public long getBloomFilterFalsePositiveCount()
    {
        return bloomFilterTracker.getFalsePositiveCount();
    }

    public long getRecentBloomFilterFalsePositiveCount()
    {
        return bloomFilterTracker.getRecentFalsePositiveCount();
    }

    public long getBloomFilterTruePositiveCount()
    {
        return bloomFilterTracker.getTruePositiveCount();
    }

    public long getRecentBloomFilterTruePositiveCount()
    {
        return bloomFilterTracker.getRecentTruePositiveCount();
    }
}
