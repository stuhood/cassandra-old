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

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.SSTable.SliceMark;
import org.apache.cassandra.io.SSTableReader.Block;

import org.apache.log4j.Logger;

/**
 * A Scanner is an abstraction for reading slices from an SSTable. In this
 * implementation, slices are read in forward order.
 */
public class SSTableScanner implements Closeable
{
    private static Logger logger = Logger.getLogger(SSTableScanner.class);

    private final BufferedRandomAccessFile file;
    private final SSTableReader sstable;
    private final ColumnKey.Comparator comparator;

    /**
     * Current block and slice pointers: the block should never be null, but
     * the slice might be if a seek*() failed.
     */
    private Block block;
    private SliceMark slice;
    private List<Column> sliceCols;

    /**
     * To acquire a Scanner over an SSTable, call SSTReader.getScanner().
     */
    SSTableScanner(SSTableReader sstable, long blockPosition) throws IOException
    {
        this.sstable = sstable;
        comparator = sstable.getComparator();
        // TODO: configurable buffer size.
        file = new BufferedRandomAccessFile(sstable.getFilename(), "r",
                                            256 * 1024);
        block = sstable.getBlock(file, blockPosition);
        slice = null;
    }

    /**
     * Releases the file handle associated with this scanner.
     */
    public void close() throws IOException
    {
        file.close();
    }

    /**
     * Seeks to the first slice  given key. See the contract for seekTo(CK).
     */
    public boolean seekTo(DecoratedKey seekKey) throws IOException
    {
        return seekTo(new ColumnKey(seekKey, new byte[0][]));
    }

    /**
     * Seeks to the first slice with a key greater than or equal to the given key. If
     * such a slice does not exist, the next calls to get*() will have undefined
     * results.
     * @return False if no such Slice was found.
     */
    public boolean seekBefore(ColumnKey seekKey) throws IOException
    {
        // seek to the correct block
        if (!seekInternal(seekKey))
            return false;

        // seek forward within the block to the slice
        while (comparator.compare(seekKey, slice.currentKey) > 0)
            if (!next())
                // TODO: assert that this loop never seeks outside of a block
                // reached the end of the file without finding a match
                return false;

        // current slice matches
        return true;
    }

    /**
     * Seeks to the slice which would contain the given key. If such a slice does not
     * exist, the next calls to get*() will have undefined results.
     * @return False if no such Slice was found.
     */
    public boolean seekTo(ColumnKey seekKey)
    {
        // seek to the first slice greater than or equal to the key
        if (!seekBefore(seekKey))
            return false;

        // confirm that the slice might contain the key
        if (slice.nextKey == null)
            // last slice in file
            return true;
        return comparator.compare(seekKey, slice.nextKey) < 0;
    }

    /**
     * Seeks to the proper block for the given seekKey, and opens the first slice.
     * FIXME: optimize forward seeks within the same block by not resetting the block
     */
    private boolean seekInternal(ColumnKey seekKey)
    {
        long position = sstable.getBlockPosition(seekKey);
        if (position < 0)
            return false;

        if (position != block.offset)
            // acquire the new block
            block = sstable.getBlock(file, position);
        block.reset();

        // read the first slice from the block
        slice = SliceMark.deserialize(block.stream());
        sliceCols = null;

        return true;
    }

    /**
     * @return In constant time, the first ColumnKey contained in the next Slice, or
     * null if we're not positioned at a slice, or are positioned at the last Slice
     * in the file. Call next() to seek to the next Slice.
     */
    public ColumnKey peek()
    {
        Slice slice = get();
        if (slice == null)
            return null;
        return slice.nextKey;
    }

    /**
     * @return The Slice at our current position, or null if the last call to
     * seekTo failed, or we're at EOF.
     */
    public Slice get()
    {
        return slice;
    }

    /**
     * @return The sorted columns for the Slice at our current position, or null if
     * the last call to seekTo failed, or we're at EOF.
     */
    public List<Column> getColumns() throws IOException
    {
        if (slice == null)
            // not positioned at a slice
            return null;
        if (sliceCols == null)
        {
            // lazily deserialize the columns
            Column[] cols = new Column[slice.numCols];
            DataInputStream stream = block.stream();
            for (int i = 0; i < cols.length; i++)
                cols[i] = (Column)Column.serializer().deserialize(stream);
            sliceCols = Arrays.asList(cols);
        }
        return sliceCols;
    }

    /**
     * Seeks to the next slice, unless the last call to seek*() failed, or we are at
     * the end of the file.
     * @return True if we are positioned at a valid slice.
     */
    public boolean next() throws IOException
    {
        if (slice == null)
            // position is invalid
            return false;
        if (slice.nextKey == null)
            // end of file: no more slices
            return false;

        // skip the remainder of the current slice
        SliceMark oldSlice = slice; // heh
        if (sliceCols == null)
            // NB: because we don't know the disk length of the block, we have to
            // skip on the stream, rather than on the file, meaning that we may
            // decompress the last slice of a block unnecessarily
            block.stream().skip(oldSlice.length);

        // seek to the next slice
        if (oldSlice.status == SliceMark.BLOCK_END)
            // positioned at the beginning of a new block
            block = sstable.getBlock(file, file.getFilePointer());
        
        // finally, read the slice
        slice = SliceMark.deserialize(block.stream());
        sliceCols = null;
    }
}
