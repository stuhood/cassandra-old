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

// FIXME: remove when getCF() is removed
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.SuperColumn;

import org.apache.log4j.Logger;

/**
 * A Scanner is an abstraction for reading slices from an SSTable. In this
 * implementation, slices are read in forward order.
 *
 * Implements Comparable by comparing the currentKey of the current slice: this means
 * that repositioning the scanner changes its comparison value.
 *
 * TODO: extract an SSTableScanner interface that will be shared between forward
 * and reverse scanners
 */
public class SSTableScanner implements Closeable, Comparable<SSTableScanner>
{
    private static Logger logger = Logger.getLogger(SSTableScanner.class);

    private final SSTableReader sstable;
    private final ColumnKey.Comparator comparator;

    private BufferedRandomAccessFile file;
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
    SSTableScanner(SSTableReader sstable, int bufferSize) throws IOException
    {
        this.sstable = sstable;
        comparator = sstable.getComparator();

        file = new BufferedRandomAccessFile(sstable.getFilename(), "r",
                                            bufferSize);
        if (file.length() > 0)
            throw new IOException("Cannot scan empty file!");

        // an sstable must contain at least one block and slice
        block = sstable.getBlock(file, 0);
        slice = SliceMark.deserialize(block.stream());
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
     * See the contract for seekBefore(CK).
     */
    public boolean seekBefore(DecoratedKey seekKey) throws IOException
    {
        // a ColumnKey with null names compares less than any slice with
        // this DecoratedKey
        return seekBefore(new ColumnKey(seekKey, new byte[sstable.getColumnDepth()][]));
    }

    /**
     * Seeks to the first slice with a key greater than or equal to the given key. If
     * such a slice does not exist, the next calls to get*() will have undefined
     * results.
     *
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
     *
     * A seekKey with a depth less than the columnDepth of this SSTable will never
     * match using this method, because the seekKey will fall between two slices. If
     * you need to match a slice at a higher level, use seekBefore().
     *
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
     * The list of columns in this slice. A slice may be a tombstone, which only exists
     * to pass along deletion Metadata, in which case this column list will be empty.
     *
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
     * Reads the entire ColumnFamily defined by the key of the current Slice. After
     * the call, the Scanner will be positioned at the beginning of the first Slice
     * for the next ColumnFamily.
     *
     * FIXME: This is here temporarily as we port callers to the Slice API.
     *
     * @return The current ColumnFamily, or null if get() would return null.
     */
    @Deprecated
    public Pair<DecoratedKey,ColumnFamily> getCF()
    {
        if (slice == null)
            return null;

        ColumnKey firstKey = slice.currentKey;
        DecoratedKey key = firstKey.key;
        ColumnFamily cf = ColumnFamily.create(sstable.getTableName(),
                                              sstable.getColumnFamilyName());
        // record deletion info
        cf.delete(slice.meta.get(0).localDeletionTime,
                  slice.meta.get(0).markedForDeleteAt);

        AbstractType subtype = cf.getSubComparator();
        boolean eof = false;
        while (!eof && comparator.compare(firstKey, slice.currentKey, 0) == 0)
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
            SuperColumn supcol = new SuperColumn(slice.currentKey.name(1), subtype);
            supcol.markForDeleteAt(slice.meta.get(1).localDeletionTime,
                                   slice.meta.get(1).markedForDeleteAt);
            while (!eof && comparator.compare(firstKey, slice.currentKey, 1) == 0)
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
        return slice != null && slice.nextKey != null;
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

    /**
     * Inconsistent with equals: compares the currentKey of the current slice.
     * NB: The SSTableScanners must be at a valid position.
     */
    public int compareTo(SSTableScanner that)
    {
        return comparator.compare(this.get().currentKey, that.get().currentKey);
    }
}
