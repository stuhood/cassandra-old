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

package org.apache.cassandra.io;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.*; // FIXME: remove when we remove flatteningAppend

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.config.DatabaseDescriptor;

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
 * SliceMark objects are written _at_least_ at the beginning of every 'natural'
 * subrange: for instance, for a column family of type super, there are SliceMarks
 * surrounding each set of subcolumns, but if the subcolumns for one column overflow
 * the end of a block (of target size MAX_BLOCK_BYTES), an additional 'artificial'
 * SliceMark will mark the beginning of the next block. See shouldFlushSlice().
 *
 * TODO: absolutely, positutely need an ascii diagram here.
 */
public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    /**
     * The target decompressed size of a block. An entire block might need to be
     * read from disk in order to read a single column. If a column is large
     * enough, the block containing it might be stretched to larger than this value.
     * FIXME: tune and make configurable
     */
    public static final int TARGET_MAX_BLOCK_BYTES = 1 << 14;

    enum BoundaryType
    {
        NONE,
        // column parents have changed
        NATURAL,
        // metadata has changed, or size limits were reached
        ARTIFICIAL
    }

    // buffer for data contained in the current slice/block
    private BlockContext blockContext;

    private long columnsWritten;
    private long slicesWritten;
    private int blocksWritten;

    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;

    private ColumnKey lastWrittenKey;
    private IndexEntry lastIndexEntry;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));

        // block metadata
        blockContext = new BlockContext();

        // etc
        columnsWritten = 0;
        slicesWritten = 0;
        blocksWritten = 0;
        // TODO: assumes ~11 columns per key
        // TODO: fix long -> int cast
        bf = new BloomFilter((int)(keyCount*11), 15); 
        lastWrittenKey = null;
        lastIndexEntry = null;
    }

    /**
     * Flushes the current slice if it is not empty, and begins a new one with the
     * given key. A slice always begins with a SliceMark indicating the length
     * of the slice.
     */
    private boolean flushSlice(Slice.Metadata meta, BoundaryType btype, ColumnKey columnKey, boolean closeBlock) throws IOException
    {
        if (blockContext.isEmpty())
            return false;

        // flush the current slice (after prepending a mark)
        blockContext.flushSlice(dataFile, btype, columnKey, closeBlock);
        if (closeBlock) blocksWritten++;
        slicesWritten++;
        // then reset for the next slice
        blockContext.resetSlice(meta, btype, columnKey);
        return true;
    }

    /**
     * A new Slice must be created on disk if any of the following are true.
     * 1. the new key does not fall into the current subrange/slice,
     *   * aka, a 'natural' boundary: the key written in the new slice will be the
     *     first in the next subrange.
     * 2. the new key has different metadata than the current slice,
     * 3. the slice size threshold has been reached.
     *
     * At a natural boundary, the least significant name in the key will be NAME_BEGIN,
     * which rounds the beginning of the slice down to the beginning of the subrange,
     * and causes the Metadata to cover all of the subrange.
     *
     * NB: Ordering is important here: natural boundaries must always be created
     * when the parent changes, otherwise, slice keys may be out of order on disk.
     *
     * @return A BoundaryType indicating if/why the slice should be flushed.
     */
    private BoundaryType shouldFlushSlice(Slice.Metadata meta, ColumnKey columnKey)
    {
        int comparison = comparator.compare(lastWrittenKey, columnKey, columnDepth-1);
        assert comparison <= 0 : "Keys written out of order! Last written key : " +
            lastWrittenKey + " Current key : " + columnKey + " Writing to " + path;
        if (comparison < 0)
            // name changed at sliceDepth: natural boundary
            return BoundaryType.NATURAL;
        if (blockContext.getApproxSliceLength() > TARGET_MAX_SLICE_BYTES)
            // max slice length reached: artificial boundary
            return BoundaryType.ARTIFICIAL;
        if (!meta.equals(blockContext.getMeta()))
            // metadata changed: artificial boundary
            return BoundaryType.ARTIFICIAL;
        return BoundaryType.NONE;
    }

    /**
     * Prepares to buffer a series of Columns that start at and share parents with
     * ColumnKey.
     *
     * @param meta @see append.
     * @param columnKey The key that is about to be appended.
     * @return The type of boundary between blocks if a block change was caused.
     */
    private BoundaryType beforeAppend(Slice.Metadata meta, ColumnKey columnKey) throws IOException
    {
        assert columnKey != null : "Keys must not be null.";

        if (lastWrittenKey == null)
        {
            // we're beginning the first slice: natural boundary
            blockContext.resetSlice(meta, BoundaryType.NATURAL, columnKey);
            return BoundaryType.NATURAL;
        }

        // true if the current block has reached its target length
        boolean filled = TARGET_MAX_BLOCK_BYTES < blockContext.getApproxBlockLength();

        // determine if we need to flush the current slice
        boolean flushed = false;
        BoundaryType btype = shouldFlushSlice(meta, columnKey);
        if (btype != BoundaryType.NONE)
        {
            // flush the previous slice to the data file
            flushed = flushSlice(meta, btype, columnKey, filled);
            assert flushed : "Failed to flush non-empty slice!";
        }
        return flushed && filled ? btype : BoundaryType.NONE;
    }

    /**
     * Handles appending any metadata to the index and filter files after having
     * written a range beginning with the given key.
     *
     * @param key The first key of the last batch of appended columns.
     * @param btype The boundary type for a newly created block.
     */
    private void afterAppend(ColumnKey key, BoundaryType btype) throws IOException
    {
        if (lastIndexEntry != null && btype == BoundaryType.NONE)
            // this append fell into the last block: don't need a new IndexEntry
            return;
        // else: the previous block was closed, or this is the first block in the file

        // columns are buffered for a new block: write an IndexEntry to mark it
        long indexPosition = indexFile.getFilePointer();
        ColumnKey blockKey = btype == BoundaryType.NATURAL ?
            key.withName(ColumnKey.NAME_BEGIN) : key;
        lastIndexEntry = new IndexEntry(blockKey.dk, blockKey.names, indexPosition,
                                        blockContext.getCurrentBlockPos());
        lastIndexEntry.serialize(indexFile);
        if (logger.isDebugEnabled())
            logger.debug("Initialized block marked by " + lastIndexEntry + " in " + getFilename());

        // if we've written INDEX_INTERVAL blocks/IndexEntries, hold onto one in memory
        if (blocksWritten % INDEX_INTERVAL != 0)
            return;
        indexEntries.add(lastIndexEntry);
    }

    /**
     * Appends the given column to the SSTable.
     *
     * @param meta Metadata for the parents of the column. A supercf has
     *     a Metadata list of length 2, while a standard cf has length 1.
     * @param columnKey The fully qualified key for the column.
     * @param column A column to append to the SSTable, or null if the given
     *     Metadata represents a tombstone.
     */
    public void append(Slice.Metadata meta, ColumnKey columnKey, Column column) throws IOException
    {
        assert column != null;
        BoundaryType btype = beforeAppend(meta, columnKey);
        blockContext.bufferColumn(column);
        afterAppend(columnKey, btype);

        // add to filter
        columnKey.addToBloom(bf);
        lastWrittenKey = columnKey;
        columnsWritten++;
    }

    /**
     * Appends the given SliceBuffer to the SSTable. The buffer is assumed not
     * to violate SLICE_TARGET_MAX_BYTES.
     *
     * @param slice SliceBuffer to append.
     */
    public void append(SliceBuffer slice) throws IOException
    {
        assert slice != null;
        BoundaryType btype = beforeAppend(slice.meta, slice.key);
        blockContext.buffer(slice);
        afterAppend(slice.key, btype);

        // add to filter
        for (Column col : slice.realized())
            // TODO: this realizes the columns, and copies and hashes the key for each
            // one: consider unioning the bloom filters during minor compactions, and
            // building it externally for major compactions
            slice.key.withName(col.name()).addToBloom(bf);
        lastWrittenKey = slice.end;
        columnsWritten += slice.numCols();
    }

    /**
     * FIXME: Flattens a CF into a SSTableWriter: in the long term the CF structure
     * should probably be replaced in memory with something like Slice, or removed
     * altogether.
     */
    @Deprecated
    public void flatteningAppend(DecoratedKey key, ColumnFamily cf) throws IOException
    {
        Slice.Metadata meta = new Slice.Metadata(cf.getMarkedForDeleteAt(),
                                                 cf.getLocalDeletionTime());

        if (!cf.isSuper())
        {
            for (IColumn column : cf.getSortedColumns())
                append(meta, new ColumnKey(key, column.name()), (Column)column);
            return;
        }
        
        for (IColumn column : cf.getSortedColumns())
        {
            SuperColumn sc = (SuperColumn)column;
            // super columns contain an additional level of metadata
            Slice.Metadata childMeta = meta.childWith(sc.getMarkedForDeleteAt(),
                                                      sc.getLocalDeletionTime());
            for (IColumn subc : sc.getSubColumns())
            {
                /* Now write the key and column to disk */
                append(childMeta, new ColumnKey(key, sc.name(), subc.name()),
                       (Column)subc);
            }
        }
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader(double cacheFraction) throws IOException
    {
        // flush the slice and block we were writing
        flushSlice(null, BoundaryType.NATURAL, null, true);

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

        logger.info("Wrote " + blocksWritten + " blocks, " +
            slicesWritten + " slices, and " + columnsWritten + " columns to " + path);

        rename(indexFilename());
        rename(filterFilename());
        path = rename(path); // important to do this last since index & filter file names are derived from it

        return new SSTableReader(path, partitioner, indexEntries, bf,
                                (int)(cacheFraction * columnsWritten));
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
     * A mutable class representing the currently buffered slice, and the containing
     * block. All data between individual SliceMarks will be buffered here, so that
     * we can determine the length from the first mark to the second.
     */
    static class BlockContext
    {
        private Slice.Metadata meta = null;
        private ColumnKey headKey = null;
        private int numCols = 0;

        private int slicesInBlock = 0;
        private long currentBlockPos = 0;

        private DataOutputBuffer sliceBuffer = new DataOutputBuffer();
        private DataOutputStream blockStream = null;

        /**
         * Serializes and buffers the given column into the current slice.
         */
        public void bufferColumn(Column column)
        {
            Column.serializer().serialize(column, sliceBuffer);
            numCols++;
        }

        /**
         * Buffers the given slice: asserts that the current slice is empty.
         */
        public void buffer(SliceBuffer slice)
        {
            assert isEmpty();
            meta = slice.meta;
            headKey = slice.key;
            numCols = slice.numCols();
            // TODO: we copy rather than worrying about who owns the input buffer
            try
            {
                sliceBuffer.write(slice.serialized().getData(),
                                  0, slice.serialized().getLength());
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }

        /**
         * @return The metadata for the current slice, or null if a slice has not
         * been started.
         */
        public Slice.Metadata getMeta()
        {
            return meta;
        }

        /**
         * @return True if no columns are buffered for the current slice.
         */
        public boolean isEmpty()
        {
            return numCols == 0;
        }

        /**
         * @return Count of columns buffered for current slice.
         */
        public int getSliceColCount()
        {
            return numCols;
        }

        public long getCurrentBlockPos()
        {
            return currentBlockPos;
        }

        /**
         * @return The sum of the exact length of the block that has been flushed to
         * disk, and the approximate length of the currently buffered slice.
         */
        public int getApproxBlockLength()
        {
            return (blockStream != null ? blockStream.size() : 0) + getApproxSliceLength();
        }

        public int getApproxSliceLength()
        {
            return sliceBuffer.getLength();
        }

        /**
         * Closes the current block, and resets the context so that the next
         * flushed slice will be the first in a new block.
         */
        private void closeBlock(RandomAccessFile file) throws IOException
        {
            assert blockStream != null && blockStream.size() > 0 :
                "Should not write empty blocks.";

            // flush block content (without actually closing the file)
            blockStream.close();

            // reset for the next block
            blockStream = null;
            slicesInBlock = 0;
            currentBlockPos = file.getFilePointer();
        }

        /**
         * Begins a slice with the given shared metadata and first key.
         */
        public void resetSlice(Slice.Metadata meta, BoundaryType btype, ColumnKey headKey)
        {
            assert btype != BoundaryType.NONE;
            
            this.meta = meta;
            this.headKey = headKey != null && btype == BoundaryType.NATURAL ?
                headKey.withName(ColumnKey.NAME_BEGIN) : headKey;
            sliceBuffer.reset();
            numCols = 0;
        }

        /**
         * Prepend a mark to our buffer to indicate the beginning of the slice, and
         * then flush the buffered data to the given output.
         * @param closeBlock True if this should be the last slice in the block.
         */
        public void flushSlice(BufferedRandomAccessFile file, BoundaryType btype, ColumnKey nextKey, boolean closeBlock) throws IOException
        {
            assert btype != BoundaryType.NONE;
            if (slicesInBlock == 0)
            {
                // first slice in block: prepend BlockHeader, and open stream
                new BlockHeader("FIXME").serialize(file);
                assert blockStream == null;
                blockStream = new DataOutputStream(new GZIPOutputStream(file.outputStream()));
            }

            int sliceLen = sliceBuffer.getLength();
            byte status = closeBlock ? SliceMark.BLOCK_END : SliceMark.BLOCK_CONTINUE;
            // TODO: this status code usage is yucky
            ColumnKey endKey = btype == BoundaryType.NATURAL ?
                headKey.withName(ColumnKey.NAME_END) : nextKey;
            nextKey = nextKey != null && btype == BoundaryType.NATURAL ?
                nextKey.withName(ColumnKey.NAME_BEGIN) : nextKey;
            new SliceMark(meta, headKey, endKey, nextKey,
                          sliceLen, numCols, status).serialize(blockStream);
            blockStream.write(sliceBuffer.getData(), 0, sliceLen);

            // update block counts
            slicesInBlock++;

            if (closeBlock)
                closeBlock(file);
        }
    }
}
