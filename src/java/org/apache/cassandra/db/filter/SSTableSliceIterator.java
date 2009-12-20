package org.apache.cassandra.db.filter;
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


import java.util.*;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.*;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A Column Iterator over a user defined slice of columns in a key.
 *
 * FIXME: This class is basically stubbed at the moment.
 */
class SSTableSliceIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    private final boolean reversed;
    private final byte[] startColumn;
    private final byte[] finishColumn;
    private final AbstractType comparator;
    private ColumnGroupReader reader;

    public SSTableSliceIterator(SSTableReader ssTable, String key, byte[] startColumn, byte[] finishColumn, boolean reversed)
    throws IOException
    {
        this.reversed = reversed;

        // FIXME: to do this correctly, we need to implement a reverse scanner class,
        // preferably with the same interface, so that it can be used transparently
        assert !reversed : "Not implemented.";

        /* Morph key into actual key based on the partition type. */
        DecoratedKey decoratedKey = ssTable.getPartitioner().decorateKey(key);
        // FIXME long position = ssTable.getPosition(decoratedKey);
        long position = 0;
        this.comparator = ssTable.getColumnComparator();
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        if (position >= 0)
            reader = new ColumnGroupReader(ssTable, decoratedKey, position);
    }

    private boolean isColumnNeeded(IColumn column)
    {
        if (startColumn.length == 0 && finishColumn.length == 0)
            return true;
        else if (startColumn.length == 0 && !reversed)
            return comparator.compare(column.name(), finishColumn) <= 0;
        else if (startColumn.length == 0 && reversed)
            return comparator.compare(column.name(), finishColumn) >= 0;
        else if (finishColumn.length == 0 && !reversed)
            return comparator.compare(column.name(), startColumn) >= 0;
        else if (finishColumn.length == 0 && reversed)
            return comparator.compare(column.name(), startColumn) <= 0;
        else if (!reversed)
            return comparator.compare(column.name(), startColumn) >= 0 && comparator.compare(column.name(), finishColumn) <= 0;
        else // if reversed
            return comparator.compare(column.name(), startColumn) <= 0 && comparator.compare(column.name(), finishColumn) >= 0;
    }

    public ColumnFamily getColumnFamily()
    {
        return reader.getEmptyColumnFamily();
    }

    protected IColumn computeNext()
    {
        if (reader == null)
            return endOfData();

        while (true)
        {
            IColumn column = reader.pollColumn();
            if (column == null)
                return endOfData();
            if (isColumnNeeded(column))
                return column;
        }
    }

    public void close() throws IOException
    {
        if (reader != null)
            reader.close();
    }

    /**
     *  This is a reader that finds the block for a starting column and returns
     *  blocks before/after it for each next call. This function assumes that
     *  the CF is sorted by name and exploits the name index.
     *
     *  TODO: convert into google collections AbstractIterator
     */
    class ColumnGroupReader
    {
        private final ColumnFamily emptyColumnFamily;

        private final BufferedRandomAccessFile file;
        // buffer of collected columns
        private final Deque<IColumn> blockColumns = new ArrayDeque<IColumn>();
        // an iterator over index entries for this range in the proper order
        private final PeekingIterator<IndexEntry> blocksToScan;

        private final byte[] leftColumn;
        private final byte[] rightColumn;

        /**
         * @param indexEntries A subset of the SSTable index covering the relevant
         * range. If the range is unbounded, the first and last entries are guaranteed
         * to point to the first and last columns for the given key.
         */
        public ColumnGroupReader(SSTableReader ssTable, DecoratedKey key, long position) throws IOException
        {
            this.file = new BufferedRandomAccessFile(ssTable.getFilename(), "r", DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);

            file.seek(position);
            DecoratedKey keyInDisk = ssTable.getPartitioner().convertFromDiskFormat(file.readUTF());
            assert keyInDisk.equals(key);

            // FIXME
            emptyColumnFamily = ColumnFamily.create(ssTable.getTableName(), ssTable.getColumnFamilyName());
            // FIXME
            blocksToScan = Iterators.peekingIterator(Collections.<IndexEntry>emptyList().iterator());

            // within a block on disk, we always read columns from left to right, so
            // redefine startColumn and finishColumn in those terms
            leftColumn = reversed ? finishColumn : startColumn;
            rightColumn = reversed ? startColumn : finishColumn;
        }

        public ColumnFamily getEmptyColumnFamily()
        {
            return emptyColumnFamily;
        }

        public IColumn pollColumn()
        {
            IColumn column = blockColumns.poll();
            if (column == null)
            {
                try
                {
                    if (getNextBlock())
                        column = blockColumns.poll();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return column;
        }

        public boolean getNextBlock() throws IOException
        {
            if (!blocksToScan.hasNext())
                return false;

            boolean found = false;
            IColumn column = null;
            Pair<IndexEntry,IndexEntry> block = nextBlock();

            // start from block.left, and read until we see the first column we want
            file.seek(block.left.dataOffset);
            while (blocksToScan.hasNext())
            {
                column = emptyColumnFamily.getColumnSerializer().deserialize(file);
                if (leftColumn.length == 0 || comparator.compare(leftColumn, column.name()) <= 0)
                {
                    // this column is the first in the block to fall into the range!
                    found = true;
                    break;
                }

                if (file.getFilePointer() > block.right.dataOffset)
                    // this block is exhausted. generate the next
                    block = nextBlock();
            }

            if (!found)
                // no more blocks intersecting the range, and no initial column
                return false;
            bufferColumn(column);

            // read from the block until it is exhausted
            while (file.getFilePointer() <= block.right.dataOffset)
            {
                column = emptyColumnFamily.getColumnSerializer().deserialize(file);

                if (rightColumn.length != 0 && comparator.compare(rightColumn, column.name()) < 0)
                    // column is outside the range
                    break;

                // found one: continue buffering until the end of the range or block
                bufferColumn(column);
            }
            return true;
        }

        /**
         * Calls blocksToScan.next() once, and then peeks at the next entry.
         * In a reversed query for example, the iterator is reversed, so the
         * first block returned will be the last block in the range.
         *
         * @return The positions of the first and last columns in the block.
         */
        private Pair<IndexEntry, IndexEntry> nextBlock()
        {
            IndexEntry first = blocksToScan.next();
            IndexEntry second = blocksToScan.peek();
            return reversed ? new Pair(second, first) : new Pair(first, second);
        }

        /**
         * Buffers the given column for return by pollColumn
         */
        private void bufferColumn(IColumn column)
        {
            if (reversed)
                blockColumns.addFirst(column);
            else
                blockColumns.addLast(column);
        }

        public void close() throws IOException
        {
            file.close();
        }
    }
}
