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

import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.SuperColumn;

import org.apache.log4j.Logger;

/**
 * A Scanner is an abstraction for reading slices from an SSTable. In this
 * implementation, slices are read in forward order.
 *
 * After creation, the Scanner is positioned at the beginning of the file, before
 * the first slice (if it exists). Call seek() or next() to position it at Slices.
 *
 * FIXME: we should open the file lazily, since a bloom filter check in seekTo might
 * indicate that we don't need to look at the dataFile anyway.
 *
 * TODO: extract an SSTableScanner interface that will be shared between forward
 * and reverse scanners
 */
public class SSTableScanner implements Closeable
{
    private static Logger logger = Logger.getLogger(SSTableScanner.class);

    private final SSTableReader sstable;
    private final ColumnKey.Comparator comparator;

    private BufferedRandomAccessFile file;
    /**
     * Current block and slice pointers. The block is lazily created for the first
     * positioning call and remains non-null for the life of the scanner. The slice
     * will be null whenever the Scanner is not in a valid position.
     */
    private Block block;
    private long blockOff;
    private SliceMark slice;
    private SliceBuffer sliceCols;

    /**
     * To acquire a Scanner over an SSTable, call SSTReader.getScanner().
     */
    SSTableScanner(SSTableReader sstable, int bufferSize) throws IOException
    {
        this.sstable = sstable;
        comparator = sstable.getComparator();

        file = new BufferedRandomAccessFile(sstable.getFilename(), "r", bufferSize);
    }

    private void loadBlock(long position) throws IOException
    {
        block = sstable.getBlock(file, position);
        // read the header, record the block content offset
        block.header();
        blockOff = file.getFilePointer();
    }

    /**
     * @return The underlying SSTableReader.
     */
    public SSTableReader sstable()
    {
        return sstable;
    }

    /**
     * @return The column comparator of the underlying SSTable.
     */
    public ColumnKey.Comparator comparator()
    {
        return comparator;
    }

    /**
     * Releases the file handle associated with this scanner.
     */
    public void close() throws IOException
    {
        if (file != null);
            file.close();
        file = null;
    }

    /**
     * Positions the Scanner at the first slice in the file.
     * @return False if the file is empty.
     */
    public boolean first() throws IOException
    {
        if (file.length() < 1)
            return false;
        loadBlock(0);
        slice = SliceMark.deserialize(block.stream());
        return true;
    }

    /**
     * See the contract for seekNear(CK).
     */
    public boolean seekNear(DecoratedKey seekKey) throws IOException
    {
        // empty names match the beginning of the first slice with this DecoratedKey
        return seekNear(new ColumnKey(seekKey, sstable.getColumnDepth()));
    }

    /**
     * Seeks to the slice which might contain the given key, without checking the
     * existence of the key in the filter. If such a slice does not exist, the next
     * calls to get*() will have undefined results.
     *
     * seekKeys with trailing NAME_BEGIN or NAME_END names will properly match
     * the slices that they begin or end when used with this method.
     *
     * @return False if no such Slice was found.
     */
    public boolean seekNear(ColumnKey seekKey) throws IOException
    {
        return seekInternal(seekKey, false);
    }

    /**
     * See the contract for seekTo(CK).
     */
    public boolean seekTo(DecoratedKey seekKey) throws IOException
    {
        // empty names match the beginning of the first slice with this DecoratedKey
        return seekTo(new ColumnKey(seekKey, sstable.getColumnDepth()));
    }

    /**
     * Seeks to the slice which might contain the given key. If the key does not
     * exist, or such a slice does not exist, the next calls to get*() will have
     * undefined results.
     *
     * seekKeys with trailing NAME_BEGIN or NAME_END names will properly match
     * the slices that they begin or end when used with this method.
     *
     * @return False if no such Slice was found.
     */
    public boolean seekTo(ColumnKey seekKey) throws IOException
    {
        return seekInternal(seekKey, true);
    }

    /**
     * Seeks to the slice that might contain the given seekKey. If exact is true,
     * the filter is checked for the existence of the key, and the seek will fail
     * if it does not exist.
     *
     * FIXME: optimize forward seeks within the same block by not resetting the block
     */
    private boolean seekInternal(ColumnKey seekKey, boolean exact) throws IOException
    {
        long position = exact ?
            sstable.getBlockPosition(seekKey) : sstable.nearestBlockPosition(seekKey);
        if (position < 0)
            return false;

        if (block == null || position != block.offset)
            // acquire the new block
            loadBlock(position);
        else
            block.reset();

        // read the first slice from the block
        slice = SliceMark.deserialize(block.stream());
        sliceCols = null;

        // seek forward while the key is >= the beginning of the next slice
        while (slice.nextKey != null && comparator.compare(seekKey, slice.nextKey) >= 0)
            assert next();

        return true;
    }

    /**
     * @return In constant time, the first ColumnKey contained in the next Slice, or
     * null if we're not positioned at a slice, or are positioned at the last Slice
     * in the file. Call next() to seek to the next Slice.
     */
    public ColumnKey peek()
    {
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
     * A list of columns contained in this slice. A slice may be a tombstone,
     * which only exists to pass along deletion Metadata, in which case the list
     * will be empty.
     *
     * @return A column list for the slice at our current position, or null if
     * the last call to seekTo failed, or we're at EOF.
     */
    public List<Column> getColumns() throws IOException
    {
        if (slice == null)
            // not positioned at a slice
            return null;
        if (sliceCols == null)
        {
            Column[] cols = new Column[slice.numCols];
            DataInputStream stream = block.stream();
            for (int i = 0; i < cols.length; i++)
                cols[i] = (Column)Column.serializer().deserialize(stream);
            sliceCols = new SliceBuffer(slice.meta, slice.key, slice.end, cols);
        }
        return sliceCols.realized();
    }

    /**
     * A buffer containing the columns in this slice. A slice may be a tombstone,
     * which only exists to pass along deletion Metadata, in which case the buffer
     * will be empty.
     *
     * @return A SliceBuffer for the slice at our current position, or null if
     * the last call to seekTo failed, or we're at EOF.
     */
    public SliceBuffer getBuffer() throws IOException
    {
        if (slice == null)
            // not positioned at a slice
            return null;
        if (sliceCols == null)
        {
            DataOutputBuffer buff = new DataOutputBuffer(slice.length);
            buff.write(block.stream(), slice.length);
            sliceCols = new SliceBuffer(slice.meta, slice.key, slice.end,
                                        slice.numCols, buff);
        }
        return sliceCols;
    }

    /**
     * Reads the entire ColumnFamily defined by the key of the current Slice. After
     * the call, the Scanner will be positioned at the beginning of the first Slice
     * for the next ColumnFamily, or at EOF.
     *
     * FIXME: This is here temporarily as we port callers to the Slice API.
     *
     * @return The current ColumnFamily, or null if get() would return null.
     */
    @Deprecated
    public Pair<DecoratedKey,ColumnFamily> getCF() throws IOException
    {
        if (slice == null)
            return null;

        ColumnKey firstKey = slice.key;
        DecoratedKey key = firstKey.dk;
        ColumnFamily cf = ColumnFamily.create(sstable.getTableName(),
                                              sstable.getColumnFamilyName());
        // record deletion info
        cf.delete(slice.meta.get(0).localDeletionTime,
                  slice.meta.get(0).markedForDeleteAt);

        AbstractType subtype = cf.getSubComparator();
        boolean eof = false;
        while (!eof && comparator.compare(firstKey, slice.key, 0) == 0)
        {
            if (!cf.isSuper())
            {
                // standard: read all slices with the same key into one CF
                for (Column column : getColumns())
                    cf.addColumn(column);
                if (!next())
                    eof = true;
                continue;
            }

            // else: super: additional layer of metadata and names
            SuperColumn supcol = new SuperColumn(slice.key.name(1), subtype);
            supcol.markForDeleteAt(slice.meta.get(1).localDeletionTime,
                                   slice.meta.get(1).markedForDeleteAt);
            while (!eof && comparator.compare(firstKey, slice.key, 1) == 0)
            {
                for (Column column : getColumns())
                    supcol.addColumn(column);
                // seek to next slice
                if (!next())
                    // break both loops
                    eof = true;
            }
            cf.addColumn(supcol);
        }
        return new Pair(key, cf);
    }

    /**
     * @return True if we are positioned at a valid slice and a call to next() will be
     * successful.
     */
    public boolean hasNext() throws IOException
    {
        if (block == null)
            return first();
        return slice != null && slice.nextKey != null;
    }

    /**
     * Seeks to the next slice, unless the last call to seek*() failed, or we are at
     * the end of the file.
     * @return True if we are positioned at the next valid slice.
     */
    public boolean next() throws IOException
    {
        if (block == null && !first())
            // the file is empty
            return false;
        if (slice == null)
            // position is invalid
            return false;
        if (slice.nextKey == null)
        {
            // end of file: no more slices
            slice = null;
            return false;
        }

        SliceMark oldSlice = slice; // heh
        if (sliceCols == null)
            // skip the remainder of the current slice
            block.stream().skip(oldSlice.length);

        // seek to the next slice
        if (oldSlice.status == SliceMark.BLOCK_END)
            // get next block
            loadBlock(blockOff + block.header().blockLen);
        
        // finally, read the new slice
        slice = SliceMark.deserialize(block.stream());
        sliceCols = null;
        return true;
    }
}
