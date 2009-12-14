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
import java.util.ArrayList;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
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
 * SliceMark objects are written _at_least_ at the beginning and end of every
 * subrange: for instance, for a ColumnFamily of type super, there are SliceMarks
 * surrounding each set of subcolumns, but if the subcolumns for one column overflow
 * the end of a block (of max size MAX_BLOCK_BYTES), an additional SliceMark will mark
 * the end of the block, and indicate whether the subcolumns continue in the next
 * block.
 */
public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    /**
     * The target decompressed size of a block. An entire block might need to be
     * read from disk in order to read a single column. If a column is large
     * enough, the block containing it may be larger than this value.
     *
     * FIXME: current value is mostly arbitrary
     */
    public static final int TARGET_MAX_BLOCK_BYTES = 1 << 18;
    /**
     * All blocks (aside from the last one) will be at least this decompressed length.
     * If a very large column is about to be appended to a block that contains less
     * than this amount of data, the large column will be appended, violating
     * TARGET_MAX_BLOCK_BYTES rather than creating a block smaller than this value.
     *
     * FIXME: current value is mostly arbitrary
     */
    public static final int MIN_BLOCK_BYTES = 1 << 16;

    /**
     * The depth of the names in a ColumnKey that separate one slice from another.
     * For a super column family, this will be 1, since a new super column begins
     * whenever the first name changes. For a regular column family, it will be 0,
     * because only the key separates slices.
     */
    private final int sliceDepth;

    /**
     * All data between individual SliceMarks will be buffered here, so that we can
     * determine the length from the first mark to the second.
     */
    private final DataOutputBuffer sliceOutputBuffer;
    // the first ColumnKey contained in the current sliceOutputBuffer
    private ColumnKey sliceOutputKey;

    private long keysWritten;
    private int blocksWritten;
    private int approxBlockLength;
    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;

    private BloomFilter bf;
    private ColumnKey lastWrittenKey;
    private IndexEntry lastIndexEntry;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner, ColumnKey.Comparator comparator) throws IOException
    {
        super(filename, partitioner, comparator);
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));

        sliceDepth = "Super".equals(DatabaseDescriptor.getColumnFamilyType(getTableName(), getColumnFamilyName())) ? 1 : 0;

        sliceOutputBuffer = new DataOutputBuffer();
        sliceOutputKey = null;


        bf = new BloomFilter((int)keyCount, 15); // TODO fix long -> int cast
        keysWritten = 0;
        blocksWritten = 0;
        approxBlockLength = 0;
        lastWrittenKey = null;
        lastIndexEntry = null;
    }

    /**
     * Flushes the current block if it is not empty, and begins a new one with the
     * given key. Beginning a new block automatically begins a new slice.
     *
     * TODO: We could write a block tail containing checksum info.
     *
     * @return False if the block was empty.
     */
    private boolean flushBlock(ColumnKey columnKey)
    {
        if (lastWrittenKey == null && approxBlockLength == 0)
            return false;
        
        // flush the current slice...
        flushSlice();
        blocksWritten++;
        approxBlockLength = 0;

        // and cap it with a BLOCK_END mark
        SliceMark mark = new SliceMark(columnKey, SliceMark.BLOCK_END);
        mark.serialize(dataFile);
        return true;
    }

    /**
     * Flushes the current slice if it is not empty, and begins a new one with the
     * given key. A Slice always begins with a SliceMark indicating the length
     * of the slice.
     *
     * @return False if the slice was empty.
     */
    private boolean flushSlice(ColumnKey columnKey)
    {
        if (sliceOutputKey == null && sliceOutputBuffer.getLength() == 0)
            return false;
        // prepend a mark containing the length of this slice
        SliceMark mark = new SliceMark(sliceOutputKey, sliceOutputBuffer.getLength());
        mark.serialize(dataFile);
        dataFile.write(sliceOutputBuffer.getData());
        
        sliceOutputBuffer.clear();
        sliceOutputKey = columnKey;
    }

    /**
     * Handles prepending metadata to the data file before writing the given ColumnKey.
     *
     * @param columnKey The key that is about to be appended,
     * @param columnLen The length in bytes of the column that will be appended.
     *
     * TODO: This is where we could write a block header containing compression info.
     */
    private long beforeAppend(ColumnKey columnKey, int columnLen) throws IOException
    {
        assert columnKey != null : "Keys must not be null.";

        if (lastWrittenKey == null)
            // we're beginning the first slice
            return 0;

        /* Determine if we need to begin a new slice or block. */

        // flush the block if it is within thresholds
        int curBlockLen = approxBlockLength + sliceOutputBuffer.getLength();
        int expectedBlockLen = columnLen + bufferedBlockLen;
        if (MIN_BLOCK_BYTES > curBlockLen && expectedBlockLen > TARGET_MAX_BLOCK_BYTES)
        {
            // current block is at least MIN_BLOCK_BYTES long, and adding
            // this column would push it over TARGET_MAX_BLOCK_BYTES: flush.
            flushBlock(columnKey);
            return dataFile.getFilePointer();
        }

        // flush the slice if the new key does not fall into the last slice
        int comparison = comparator.compare(lastWrittenKey, columnKey, sliceDepth);
        assert comparison <= 0 : "Keys written out of order! Last written key : " +
            lastWrittenKey + " Current key : " + columnKey + " Writing to " + path;
        if (comparison < 0)
        {
            // flush the previous slice to the data file
            flushSlice(columnKey);
            return dataFile.getFilePointer();
        }

        // we're still in the same slice
        return dataFile.getFilePointer();
    }

    /**
     * Handles appending any metadata to the data or index file after having
     * written the given ColumnKey.
     */
    private void afterAppend(ColumnKey columnKey, int columnLen, long blockPosition) throws IOException
    {
        // FIXME: bloomfilter should contain entire ColumnKey
        bf.add(columnKey);
        lastWrittenKey = columnKey;
        keysWritten++;

        // this block length is approximate because we don't include the
        // overhead of serialized SliceMark objects among the columns
        approxBlockLength += columnLen;
        
        if (logger.isTraceEnabled())
            logger.trace("Wrote " + columnKey + " in block at " + blockPosition);
        if (lastIndexEntry != null && lastIndexEntry.dataOffset == blockPosition)
            // this append fell into the last block: don't need a new IndexEntry
            return;

        // write an IndexEntry for the new block
        long indexPosition = indexFile.getFilePointer();
        IndexEntry entry = new IndexEntry(columnKey.key, columnKey.names,
                                          indexPosition, blockPosition);
        entry.serialize(indexFile);

        // if we've written INDEX_INTERVAL IndexEntries, hold onto one in memory
        if (blocksWritten % INDEX_INTERVAL != 0)
            return;
        indexEntries.add(entry);
        if (logger.isTraceEnabled())
            logger.trace("Wrote IndexEntry for " + columnKey + " at position " + indexPosition + " for block at " + blockPosition);
    }

    /**
     * Takes a ColumnKey and a serialized column, and appends the column to the end of
     * the data file, possibly marking it with a ColumnMarker.
     */
    public void append(ColumnKey columnKey, DataOutputBuffer buffer) throws IOException
    {
        append(columnKey, buffer.getData());
    }

    public void append(ColumnKey columnKey, byte[] value) throws IOException
    {
        assert value.length > 0;
        long currentPosition = beforeAppend(columnKey, value.length);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        dataFile.writeInt(value.length);
        dataFile.write(value);
        afterAppend(decoratedKey, value.length, currentPosition);
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader(double cacheFraction) throws IOException
    {
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
}
