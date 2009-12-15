package org.apache.cassandra.io;
/*
 * 
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
 * 
 */


import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.*; // FIXME: remove when we remove flatteningAppend

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.config.DatabaseDescriptor;
import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

/**
 * An SSTable is made up of an 'index', 'filter' and 'data' file.
 *
 * The index file contains a sequence of IndexEntries which point to the positions
 * of blocks in the data file.
 *
 * The data file contains a sequence of blocks. Blocks contain series of Columns,
 * which are separated by SliceMark objects which can be used for skipping through the
 * block.
 *
 * SliceMark objects are written _at_least_ at the beginning of every
 * subrange: for instance, for a ColumnFamily of type super, there are SliceMarks
 * surrounding each set of subcolumns, but if the subcolumns for one column overflow
 * the end of a block (of target size MAX_BLOCK_BYTES), an additional SliceMark will
 * mark the end of the block, and indicate whether the subcolumns continue in the next
 * block.
 *
 * TODO: Wishlist:
 * * Add a block header containing metadata, such as the compression type?
 * * Align more row keys with the beginnings of blocks by flushing blocks on
 *   key changes falling between MIN and MAX.
 * * ColumnKey.Comparator could encode the depth of difference in its return
 *   value so we can accomplish the previous bullet without an extra compare.
 * * Reduce buffer copies in append() by buffering Column objects (instead of buffers),
 *   which we can serialize directly to the block.
 */
public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    /**
     * The target decompressed size of a block. An entire block might need to be
     * read from disk in order to read a single column. If a column is large
     * enough, the block containing it might be stretched to larger than this value.
     * TODO: tune
     */
    public static final int TARGET_MAX_BLOCK_BYTES = 1 << 18;
    /**
     * All blocks (aside from the last one) will be at least this decompressed length.
     * If a very large column is about to be appended to a block that contains less
     * than this amount of data, TARGET_MAX_BLOCK_BYTES will be ignored rather than
     * creating a block smaller than this value.
     * TODO: tune
     */
    public static final int MIN_BLOCK_BYTES = 1 << 16;
    /**
     * The target maximum size of a Slice in bytes (between two SliceMarks in a block).
     * SliceMarks allow skipping columns within a block, but this many bytes will need
     * to be held in memory while writing the SSTable. Large columns will stretch this
     * value (because a slice cannot be smaller than a column).
     * TODO: tune
     */
    public static final int TARGET_MAX_SLICE_BYTES = 1 << 14;

    /**
     * The depth of the names in a ColumnKey that separate one slice from another.
     * For a super column family, this will be 1, since a new super column begins
     * whenever the first name changes. For a regular column family, it will be 0,
     * because only the key separates slices.
     */
    private final int sliceDepth;

    // buffer for data contained in the current slice
    private SliceContext sliceContext;

    // the disk position of the start of the current block, and its approx length
    private long currentBlockPos;
    private int approxBlockLen;

    private long keysWritten;
    private int blocksWritten;
    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;

    private BloomFilter bf;
    private ColumnKey lastWrittenKey;
    private IndexEntry lastIndexEntry;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));

        // slice metadata
        sliceDepth = "Super".equals(DatabaseDescriptor.getColumnFamilyType(getTableName(), getColumnFamilyName())) ? 1 : 0;
        sliceContext = new SliceContext();

        // block metadata
        approxBlockLen = 0;
        currentBlockPos = 0;

        // etc
        keysWritten = 0;
        blocksWritten = 0;
        bf = new BloomFilter((int)keyCount, 15); // TODO fix long -> int cast
        lastWrittenKey = null;
        lastIndexEntry = null;
    }

    /**
     * Flushes the current block if it is not empty, and begins a new one with the
     * given key. Beginning a new block automatically begins a new slice. Passing
     * a null columnKey indicates the end of the file, and will write a SliceMark
     * with a null 'nextKey' value.
     *
     * TODO: We could write a block tail containing checksum info here. Perhaps
     * SliceMark should contain more generic metadata to fill this role.
     *
     * @return False if the block was empty.
     */
    private boolean flushBlock(List<Pair<Long,Integer>> parentMeta, ColumnKey columnKey) throws IOException
    {
        if (lastWrittenKey == null && approxBlockLen == 0)
            return false;
        
        // flush the current slice, and cap the block with a BLOCK_END mark:
        // BLOCK_END indicates the end of the block, and contains the first key from
        // the next block, so that a reader can determine if they need to continue
        flushSlice(parentMeta, columnKey);
        SliceMark mark = new SliceMark(lastWrittenKey, columnKey, SliceMark.BLOCK_END);
        mark.serialize(dataFile);

        // reset for the next block
        blocksWritten++;
        approxBlockLen = 0;
        currentBlockPos = dataFile.getFilePointer();
        return true;
    }

    /**
     * Flushes the current slice if it is not empty, and begins a new one with the
     * given key. A slice always begins with a SliceMark indicating the length
     * of the slice.
     *
     * @return False if the slice was empty.
     */
    private boolean flushSlice(List<Pair<Long,Integer>> parentMeta, ColumnKey columnKey) throws IOException
    {
        if (sliceContext.isEmpty())
            return false;

        // flush the currently slice (after prepending a mark)
        sliceContext.markAndFlush(dataFile, columnKey);
        // then reset for the next slice
        sliceContext.reset(parentMeta, columnKey);
        return true;
    }

    /**
     * Handles prepending metadata to the data file before writing the given ColumnKey.
     *
     * TODO: This is where we could write a block header containing compression info.
     *
     * @param parentMeta @see append.
     * @param columnKey The key that is about to be appended,
     * @param columnLen The length in bytes of the column that will be appended.
     */
    private void beforeAppend(List<Pair<Long,Integer>> parentMeta, ColumnKey columnKey, int columnLen) throws IOException
    {
        assert columnKey != null : "Keys must not be null.";

        if (lastWrittenKey == null)
        {
            // we're beginning the first slice
            sliceContext.reset(parentMeta, columnKey);
            return;
        }

        /* Determine if we need to begin a new block or slice. */

        // flush the block if it is within thresholds
        int curBlockLen = approxBlockLen + sliceContext.getLength();
        int expectedBlockLen = columnLen + curBlockLen;
        if (MIN_BLOCK_BYTES < curBlockLen && expectedBlockLen > TARGET_MAX_BLOCK_BYTES)
        {
            // current block is at least MIN_BLOCK_BYTES long, and adding
            // this column would push it over TARGET_MAX_BLOCK_BYTES: flush.
            flushBlock(parentMeta, columnKey);
            return;
        }

        // flush the slice if the new key does not fall into the last slice, or if
        // adding this key would violate TARGET_MAX_SLICE_BYTES for the current slice
        int comparison = comparator.compare(lastWrittenKey, columnKey, sliceDepth);
        assert comparison <= 0 : "Keys written out of order! Last written key : " +
            lastWrittenKey + " Current key : " + columnKey + " Writing to " + path;
        if (comparison < 0 || sliceContext.getLength() + columnLen > TARGET_MAX_SLICE_BYTES)
        {
            // flush the previous slice to the data file
            flushSlice(parentMeta, columnKey);
            return;
        }
    }

    /**
     * Handles appending any metadata to the index and filter files after having
     * written the given ColumnKey to the data file.
     */
    private void afterAppend(ColumnKey columnKey, int columnLen) throws IOException
    {
        bf.add(comparator.forBloom(columnKey));
        lastWrittenKey = columnKey;
        keysWritten++;

        // this block length is approximate because we don't include the
        // overhead of serialized SliceMark objects among the columns
        approxBlockLen += columnLen;
        
        if (lastIndexEntry != null && lastIndexEntry.dataOffset == currentBlockPos)
            // this append fell into the last block: don't need a new IndexEntry
            return;

        // write an IndexEntry for the new block
        long indexPosition = indexFile.getFilePointer();
        lastIndexEntry = new IndexEntry(columnKey.key, columnKey.names,
                                        indexPosition, currentBlockPos);
        lastIndexEntry.serialize(indexFile);
        if (logger.isTraceEnabled())
            logger.trace("Wrote " + lastIndexEntry);

        // if we've written INDEX_INTERVAL blocks/IndexEntries, hold onto one in memory
        if (blocksWritten % INDEX_INTERVAL != 0)
            return;
        indexEntries.add(lastIndexEntry);
    }

    /**
     * Appends the given column to the SSTable.
     *
     * @param parentMeta A list of Pairs of ("markedForDeleteAt","localDeletionTime")
     *     values for parents of the column. A supercf will have two Pairs, and a
     *     standard cf will have one Pair. FIXME: I don't like passing the parameters
     *     this way, but we need to store them once per Slice, and the caller
     *     shouldn't have to know anything about Slices. At the absolute minimum,
     *     we should replace Pair with a purpose built metadata object.
     * @param columnKey The fully qualified key for the column.
     * @param buffer A buffer to copy the serialized column from.
     */
    public void append(List<Pair<Long,Integer>> parentMeta, ColumnKey columnKey, DataOutputBuffer buffer) throws IOException
    {
        int columnLen = buffer.getLength();
        assert columnLen > 0;
        beforeAppend(parentMeta, columnKey, columnLen);
        sliceContext.bufferColumn(buffer.getData(), columnLen);
        afterAppend(columnKey, columnLen);
    }

    /**
     * FIXME: inefficent method for flattening a CF into a SSTableWriter: in the long
     * term the CF structure should probably turn into pure metadata that describes
     * a range in a Memtable or SSTable, rather than actually containing columns.
     */
    @Deprecated
    public void flatteningAppend(DecoratedKey key, ColumnFamily cf) throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        // super cfs have 2 levels of nested deletion info, standard cfs have 1
        List<Pair<Long,Integer>> parentMeta = new ArrayList<Pair<Long,Integer>>(2);
        parentMeta.add(new Pair<Long,Integer>(cf.getMarkedForDeleteAt(),
                                              cf.getLocalDeletionTime()));

        if (!cf.isSuper())
        {
            for (IColumn column : cf.getSortedColumns())
            {
                buffer.reset();
                Column.serializer().serialize(column, buffer);
                append(parentMeta, new ColumnKey(key, column.name()), buffer);
            }
            return;
        }
        
        // super columns contain an additional level of metadata
        parentMeta.add(null); 
        for (IColumn column : cf.getSortedColumns())
        {
            SuperColumn sc = (SuperColumn)column;
            parentMeta.set(1, new Pair<Long,Integer>(sc.getMarkedForDeleteAt(),
                                                     sc.getLocalDeletionTime()));
            for (IColumn subc : sc.getSubColumns())
            {
                buffer.reset();
                Column.serializer().serialize(subc, buffer);
                /* Now write the key and column to disk */
                append(parentMeta, new ColumnKey(key, sc.name(), subc.name()), buffer);
            }
        }
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader(double cacheFraction) throws IOException
    {
        // flush the block we were writing
        flushBlock(null, null);

        // bloom filter
        FileOutputStream fos = new FileOutputStream(filterFilename());
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();

        // index
        indexFile.getChannel().force(true);
        indexFile.close();

        // main data
        dataFile.close(); // calls force

        rename(indexFilename());
        rename(filterFilename());
        path = rename(path); // important to do this last since index & filter file names are derived from it

        ConcurrentLinkedHashMap<ColumnKey, IndexEntry> keyCache = cacheFraction > 0
                                                        ? SSTableReader.createKeyCache((int) (cacheFraction * keysWritten))
                                                        : null;
        return new SSTableReader(path, partitioner, indexEntries, bf, keyCache);
    }

    static String rename(String tmpFilename)
    {
        String filename = tmpFilename.replace("-" + SSTable.TEMPFILE_MARKER, "");
        try
        {
            FBUtilities.renameWithConfirm(tmpFilename, filename);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return filename;
    }

    public static SSTableReader renameAndOpen(String dataFileName) throws IOException
    {
        SSTableWriter.rename(indexFilename(dataFileName));
        SSTableWriter.rename(filterFilename(dataFileName));
        dataFileName = SSTableWriter.rename(dataFileName);
        return SSTableReader.open(dataFileName, StorageService.getPartitioner(), DatabaseDescriptor.getKeysCachedFraction(parseTableName(dataFileName)));
    }

    /**
     * A mutable class representing the currently buffered slice. All data between
     * individual SliceMarks will be buffered here, so that we can determine the
     * length from the first mark to the second.
     */
    static class SliceContext
    {
        private List<Pair<Long,Integer>> parentMeta = null;
        private ColumnKey headKey = null;
        private DataOutputBuffer sliceBuffer = new DataOutputBuffer();

        public SliceContext()
        {
            this.parentMeta = parentMeta;
            this.headKey = headKey;
        }

        /**
         * Buffers bytes up to len as a serialized 'Column' object to be written to
         * the current slice.
         */
        public void bufferColumn(byte[] column, int len)
        {
            try
            {
                sliceBuffer.write(column, 0, len);
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }

        /**
         * Returns true if no slice has been initialized.
         */
        public boolean isEmpty()
        {
            return headKey == null || sliceBuffer.getLength() < 1;
        }

        public int getLength()
        {
            return sliceBuffer.getLength();
        }

        /**
         * Begins a slice with the given shared metadata and first key.
         */
        public void reset(List<Pair<Long,Integer>> parentMeta, ColumnKey headKey)
        {
            this.parentMeta = parentMeta != null ?
                new LinkedList<Pair<Long,Integer>>(parentMeta) : null;
            this.headKey = headKey;
            sliceBuffer.reset();
        }

        /**
         * Prepend a mark to our buffer to indicate the beginning of the slice, and
         * then flush the buffered data to the given output.
         */
        public void markAndFlush(DataOutput dos, ColumnKey nextKey) throws IOException
        {
            int sliceLen = sliceBuffer.getLength();
            new SliceMark(parentMeta, headKey, nextKey, sliceLen).serialize(dos);
            dos.write(sliceBuffer.getData(), 0, sliceLen);
        }
    }
}
