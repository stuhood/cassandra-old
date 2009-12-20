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

import java.io.IOException;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Arrays;

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

    // current block and slice pointers
    private Block block;
    private SliceMark slice;

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
        block = sstable.new Block(file, blockPosition);
        slice = null;
    }

    public void close() throws IOException
    {
        file.close();
    }

    /**
     * Seeks to the first slice for the given key. See the contract for seekTo(CK).
     */
    public boolean seekTo(DecoratedKey seekKey)
    {
        return seekTo(new ColumnKey(seekKey, new byte[0][]));
    }

    /**
     * Seeks to the slice containing the given key. If such a slice does not
     * exist, the next calls to the getSlice methods will be invalid.
     *
     * TODO: optimize forward seeks within the same block by not reopening
     *       the block
     * @return False if no such Slice was found.
     */
    public boolean seekTo(ColumnKey seekKey)
    {
        try
        {
            long position = sstable.getBlockPosition(seekKey);
            if (position < 0)
                return false;

            if (position != block.offset)
                // acquire the new block
                block = sstable.new Block(file, position);
            block.reset();

            // seek forward within the block to the slice
            return seekForwardInBlock(seekKey);
        }
        catch (IOException e)
        {
            throw new RuntimeException("corrupt sstable", e);
        }
        return true;
    }

    /**
     * Called internally when we're positioned at the beginning of the only block
     * which could possibly contain the given key.
     */
    private boolean seekForwardInBlock(ColumnKey seekKey)
    {
        int distance = 0;
        do
        {
            block.stream().skip(distance);
            slice = SliceMark.deserialize(block.stream());
            if (comparator.compare(slice.currentKey, seekKey) <= 0 &&
                comparator.compare(seekKey, slice.nextKey) < 0)
                // positioned at the beginning of the slice containing the key
                return true;
            
        } // seek past the current slice
        while ((distance = slice.nextMark) != -1);
       
        throw new AssertionError("Began seeking within an incorrect block.");
    }

    /**
     * @return The Slice at our current position, or null if the last call to
     * seekTo failed, or we're at EOF.
     */
    public Slice getSlice()
    {
        return slice;
    }

    /**
     * Return the columns for the Slice at our current position, or null if the
     * last call to seekTo failed, or we're at EOF.
     */
    public Iterable<Column> getSliceColumns()
    {
        throw new RuntimeException("Not implemented!"); // FIXME
    }

    /**
     * Moves to the next slice, unless we are at the end of the file.
     */
    public boolean next()
    {
        throw new RuntimeException("Not implemented!"); // FIXME
    }
}
