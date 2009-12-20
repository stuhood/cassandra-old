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
import java.nio.channels.FileChannel;
import java.util.*;

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
 * SliceMark objects are written _at_least_ at the beginning of every
 * subrange: for instance, for a ColumnFamily of type super, there are SliceMarks
 * surrounding each set of subcolumns, but if the subcolumns for one column overflow
 * the end of a block (of target size MAX_BLOCK_BYTES), an additional SliceMark will
 * mark the end of the block, and indicate whether the subcolumns continue in the next
 * block.
 *
 * FIXME: The index currently contains an IndexEntry per block, which will be
 * significantly fewer than before. If we think it is advantageous to have a 
 * "covering" index containing all keys, then we'll want to write an IndexEntry
 * per key, meaning multiple entries pointing to each block.
 *
 * TODO: Wishlist:
 * * Align more row keys with the beginnings of blocks by flushing blocks on
 *   key changes falling within a threshold of MAX.
 */
public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    /**
     * The target decompressed size of a block. An entire block is buffered in
     * memory while writing, and an entire block might need to be read from disk in
     * order to read a single column. If a slice is large enough, the block
     * containing it might be stretched to larger than this value.
     * TODO: tune
     */
    public static final int TARGET_MAX_BLOCK_BYTES = 1 << 16;
    /**
     * The target maximum size of a Slice in bytes (between two SliceMarks in a block).
     * SliceMarks allow skipping columns within a block, but this many bytes will need
     * to be held in memory while writing or reading the SSTable. Large columns will
     * stretch this value (because a slice cannot be smaller than a column).
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

    // the disk position of the start of the current block
    private long currentBlockPos;
    // buffer for data contained in the current block
    private BlockContext blockContext;

    private long keysWritten;
    private int blocksWritten;

    private FileChannel dataFileChannel;
    private CountedOutputStream dataFile;
    private FileChannel indexFileChannel;
    private CountedOutputStream indexFile;

    private BloomFilter bf;
    private ColumnKey lastWrittenKey;
    private IndexEntry lastIndexEntry;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);

        open();

        // slice metadata
        sliceDepth = "Super".equals(DatabaseDescriptor.getColumnFamilyType(getTableName(), getColumnFamilyName())) ? 1 : 0;
        sliceContext = new SliceContext();

        // block metadata
        currentBlockPos = 0;
        blockContext = new BlockContext();

        // etc
        keysWritten = 0;
        blocksWritten = 0;
        bf = new BloomFilter((int)keyCount, 15); // TODO fix long -> int cast
        lastWrittenKey = null;
        lastIndexEntry = null;
    }

    /**
     * Initializes the data and index files.
     * FIXME: ack... DataOutputStream uses an int to represent the bytes written:
     *        need to implement a FilterOutputStream below a DOS
     * FIXME: because each block is buffered in memory anyway, we should explore
     *        removing the buffering on dataFile to eliminate copies
     */
    private void open() throws IOException
    {
        int dataBufferBytes = (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024);
        FileOutputStream datafstream = new FileOutputStream(path);
        dataFileChannel = datafstream.getChannel();
        dataFile = new CountedOutputStream(new BufferedOutputStream(datafstream,
                                                                    dataBufferBytes));

        int indexBufferBytes = (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024);
        FileOutputStream indexfstream = new FileOutputStream(indexFilename());
        indexFileChannel = indexfstream.getChannel();
        indexFile = new CountedOutputStream(new BufferedOutputStream(indexfstream,
                                                                     indexBufferBytes));
    }

    /**
     * Closes the current block if it is not empty, and begins a new one with the
     * given key. Passing a null columnKey indicates the end of the file, and will
     * write a SliceMark with a null 'nextKey' value.
     *
     * @return False if the block was empty.
     */
    private boolean closeBlock(ColumnKey columnKey) throws IOException
    {
        if (lastWrittenKey == null || blockContext.isEmpty())
            return false;

        // write an index entry for the block
        long indexPosition = indexFile.written();
        ColumnKey firstKey = blockContext.firstSlice().currentKey;
        lastIndexEntry = new IndexEntry(firstKey.key, firstKey.names,
                                        indexPosition, currentBlockPos);
        lastIndexEntry.serialize(indexFile);

        // flush the block
        blockContext.markAndFlush(dataFile, lastWrittenKey, columnKey);
        if (logger.isDebugEnabled())
            logger.debug("Wrote block marked by " + lastIndexEntry + " to " + getFilename());

        // hold every INDEX_INTERVAL IndexEntries in memory
        if (blocksWritten % INDEX_INTERVAL == 0)
            indexEntries.add(lastIndexEntry);

        // reset for the next block
        blocksWritten++;
        currentBlockPos = dataFile.written();
        return true;
    }

    /**
     * Flushes the current slice if it is not empty, and begins a new one with the
     * given key. A slice always begins with a SliceMark indicating the length
     * of the slice.
     *
     * @return True if a slice was flushed.
     */
    private boolean flushSlice(Slice.Metadata parentMeta, ColumnKey columnKey) throws IOException
    {
        if (sliceContext.isEmpty())
            return false;

        // close the current slice, and buffer it to the current block
        blockContext.appendSlice(sliceContext.close(columnKey));

        // then reset for the next slice
        sliceContext.reset(parentMeta, columnKey);
        return true;
    }

    /**
     * Handles prepending metadata to the data file before writing the given ColumnKey.
     *
     * @param parentMeta @see append.
     * @param columnKey The key that is about to be appended,
     */
    private void beforeAppend(Slice.Metadata parentMeta, ColumnKey columnKey) throws IOException
    {
        assert columnKey != null : "Keys must not be null.";

        if (lastWrittenKey == null)
        {
            // we're beginning the first block and slice
            sliceContext.reset(parentMeta, columnKey);
            return;
        }

        // flush the slice if the new key does not fall into the last slice, or if
        // TARGET_MAX_SLICE_BYTES for the current slice has already been reached
        // TODO: we could micro-optimize to skip this comparison by comparing the
        // parentMeta objects for reference equality first
        int comparison = comparator.compare(lastWrittenKey, columnKey, sliceDepth);
        assert comparison <= 0 : "Keys written out of order! Last written key : " +
            lastWrittenKey + " Current key : " + columnKey + " Writing to " + path;
        if (comparison < 0 || sliceContext.getLength() > TARGET_MAX_SLICE_BYTES)
        {
            // flush the previous slice to the data file
            assert flushSlice(parentMeta, columnKey) :
                "Failed to flush non-empty slice!";
            if (logger.isTraceEnabled())
                logger.trace("Flushed slice marked by " + columnKey + " to " + getFilename());
        }
    }

    /**
     * Handles appending any metadata to the index and filter files after having
     * written the given ColumnKey to the data file.
     *
     * @param columnKey The key for the appended column.
     */
    private void afterAppend(ColumnKey columnKey) throws IOException
    {
        // close the block if it has reached its threshold
        if (blockContext.getApproxLength() > TARGET_MAX_BLOCK_BYTES)
        {
            // current block is at least TARGET_MAX_BLOCK_BYTES long: close.
            assert closeBlock(columnKey) : "Failed to close a non-empty block!";
        }

        // update the filter
        bf.add(comparator.forBloom(columnKey));
        lastWrittenKey = columnKey;
        keysWritten++;
    }

    /**
     * Appends the given column to the SSTable.
     *
     * @param parentMeta Metadata for the parents of the column. A supercf has
     *     a Metadata list of length 2, while a standard cf has length 1.
     * @param columnKey The fully qualified key for the column.
     * @param column A column to append to the SSTable.
     */
    public void append(Slice.Metadata parentMeta, ColumnKey columnKey, Column column) throws IOException
    {
        assert column != null;
        beforeAppend(parentMeta, columnKey);
        sliceContext.appendColumn(column);
        afterAppend(columnKey);
    }

    /**
     * FIXME: inefficent method for flattening a CF into a SSTableWriter: in the long
     * term the CF structure should probably be replaced in memory with something
     * like Slice, or removed altogether.
     */
    @Deprecated
    public void flatteningAppend(DecoratedKey key, ColumnFamily cf) throws IOException
    {
        Slice.Metadata parentMeta = new Slice.Metadata(cf.getMarkedForDeleteAt(),
                                                       cf.getLocalDeletionTime());

        if (!cf.isSuper())
        {
            for (IColumn column : cf.getSortedColumns())
                append(parentMeta, new ColumnKey(key, column.name()), (Column)column);
            return;
        }
        
        for (IColumn column : cf.getSortedColumns())
        {
            SuperColumn sc = (SuperColumn)column;
            // super columns contain an additional level of metadata
            Slice.Metadata childMeta = parentMeta.childWith(sc.getMarkedForDeleteAt(),
                                                            sc.getLocalDeletionTime());
            for (IColumn subc : sc.getSubColumns())
            {
                /* Now write the key and column to disk */
                append(childMeta, new ColumnKey(key, sc.name(), subc.name()),
                       (Column)column);
            }
        }
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader(double cacheFraction) throws IOException
    {
        // flush the slice and block we were writing
        flushSlice(null, null);
        closeBlock(null);

        // bloom filter
        FileOutputStream fos = new FileOutputStream(filterFilename());
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getChannel().force(true);
        stream.close();

        // index
        indexFile.flush();
        indexFileChannel.force(true);
        indexFile.close();

        // main data
        dataFile.flush();
        dataFileChannel.force(true);
        dataFile.close();

        rename(indexFilename());
        rename(filterFilename());
        path = rename(path); // important to do this last since index & filter file names are derived from it
        return new SSTableReader(path, partitioner, indexEntries, bf,
                                 (int)(cacheFraction * keysWritten));
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
        private Slice.Metadata parentMeta = null;
        private ColumnKey headKey = null;
        private List<Column> sliceBuffer = new ArrayList<Column>();
        private int length = 0;

        /**
         * Serializes and buffers the given column into the current slice.
         * NB: Depends on the accuracy of Column.size().
         */
        public void appendColumn(Column column)
        {
            sliceBuffer.add(column);
            length += column.size();
        }

        /**
         * Returns true if the slice current slice is empty.
         */
        public boolean isEmpty()
        {
            return headKey == null && sliceBuffer.isEmpty();
        }

        /**
         * Length of the content of the slice (not including a header).
         */
        public int getLength()
        {
            return length;
        }

        /**
         * Begins a slice with the given shared metadata and first key.
         */
        public void reset(Slice.Metadata parentMeta, ColumnKey headKey)
        {
            this.parentMeta = parentMeta;
            this.headKey = headKey;
            sliceBuffer = new ArrayList<Column>();
            length = 0;
        }

        /**
         * Close the current Slice, returning a SliceBuffer.
         */
        public SliceBuffer close(ColumnKey nextKey) throws IOException
        {
            SliceMark mark = new SliceMark(parentMeta, headKey, nextKey, length);
            return new SliceBuffer(mark, sliceBuffer);
        }
    }

    /**
     * A mutable class representing the currently buffered block. All data within a
     * block will be buffered here.
     */
    static class BlockContext
    {
        private int approxLength = 0;
        private final Queue<SliceBuffer> blockBuffer = new ArrayDeque<SliceBuffer>();

        /**
         * Buffers the given slice into the current block.
         */
        public void appendSlice(SliceBuffer slice)
        {
            blockBuffer.add(slice);
            approxLength += slice.left.nextMark;
        }

        /**
         * Asserts that the block is not empty.
         * @return The mark for the first buffered slice in the block.
         */
        public SliceMark firstSlice()
        {
            assert !isEmpty();
            return blockBuffer.peek().left;
        }

        /**
         * Returns true if the current block is empty.
         */
        public boolean isEmpty()
        {
            return blockBuffer.isEmpty();
        }

        /**
         * The current approximate length of the block, which is approximate because
         * it does not include the size of SliceMarks.
         *
         * TODO: SliceMark is hard to calculate size for efficently, because it
         * contains both Tokens and Strings, both of which require encoding: if
         * we could calculate size efficiently, there would be no need to buffer
         * to memory in buildContent().
         */
        public int getApproxLength()
        {
            return approxLength;
        }

        /**
         * Empties our internal buffer by serializing all Slices to a single opaque
         * buffer (possibly compressed).
         *
         * FIXME: use a stream defined by the codecClass here
         */
        private DataOutputBuffer buildContent(String codecClass, SliceMark endMark)
        {
            DataOutputBuffer dob = new DataOutputBuffer(approxLength);
            DataOutputStream dos = new DataOutputStream(dob.getBuffer());
            // write slice content
            try
            {
                ColumnSerializer cserializer = Column.serializer();
                while (!blockBuffer.isEmpty())
                {
                    SliceBuffer slice = blockBuffer.poll();
                    slice.left.serialize(dos);
                    for (Column slicecol : slice.right)
                        cserializer.serialize(slicecol, dos);
                }
                // close the block content with a BLOCK_END SliceMark
                endMark.serialize(dos);
                dos.flush();
            }
            catch(IOException e)
            {
                throw new AssertionError(e);
            }
            return dob;
        }

        /**
         * Prepend a header to the block, and then flush the buffered block to the
         * given output, and reset for another block.
         */
        public void markAndFlush(DataOutputStream dos, ColumnKey lastKey, ColumnKey curKey) throws IOException
        {
            // caps for the block
            String codecClass = "FIXME";
            SliceMark endMark = new SliceMark(lastKey, curKey, SliceMark.BLOCK_END);
            DataOutputBuffer serializedBlock = buildContent(codecClass, endMark);
            BlockHeader header = new BlockHeader(serializedBlock.getLength(), codecClass);

            // flush
            header.serialize(dos);
            dos.write(serializedBlock.getData(), 0, serializedBlock.getLength());

            // reset
            approxLength = 0;
        }
    }

    /**
     * A SliceBuffer represents a Slice which has been closed, but not yet flushed
     * to disk.
     *
     * Rather than storing the closed slice as a buffer of bytes, we store it as
     * Columns, which presumably need to stay in memory anyway, and which never
     * need to be copied to buffers.
     */
    static class SliceBuffer extends Pair<SliceMark,List<Column>>
    {
        public SliceBuffer(SliceMark mark, List<Column> columns)
        {
            super(mark, columns);
        }
    }
}
