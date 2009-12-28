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

package org.apache.cassandra.db.filter;

import java.util.*;
import java.io.IOError;
import java.io.IOException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.*;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.google.common.collect.AbstractIterator;

/**
 * An Iterator over the first layer of IColumns in an SSTable.
 */
class SSTableSliceIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    private final boolean reversed;
    private final ColumnKey startKey;
    private final ColumnKey finishKey;
   
    private final SSTableScanner scanner;
    private final ColumnKey.Comparator comparator;

    // unreturned columns from the previous slice
    private final Deque<IColumn> buffer = new ArrayDeque<IColumn>();

    private ColumnFamily cf;

    /**
     * An empty or null start/finish column will cause unbounded matching in
     * that direction.
     */
    public SSTableSliceIterator(SSTableReader ssTable, String key, byte[] startColumn, byte[] finishColumn, boolean reversed)
    throws IOException
    {
        this.reversed = reversed;
        assert !reversed : "Not implemented"; // FIXME: need reverse scanner here

        // convert to ColumnKey unbounded column constants
        if (startColumn == null || startColumn.length == 0)
            startColumn = ColumnKey.NAME_BEGIN;
        if (finishColumn == null || finishColumn.length == 0)
            finishColumn = ColumnKey.NAME_END;

        // morph string and columns into keys based on the partition type and depth
        DecoratedKey dk = ssTable.getPartitioner().decorateKey(key);
        startKey = new ColumnKey(dk, ssTable.getColumnDepth(), startColumn);
        finishKey = new ColumnKey(dk, ssTable.getColumnDepth(), finishColumn);
        
        // seek to the slice which might contain the first column
        scanner = ssTable.getScanner(DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
        scanner.seekTo(startKey);
        comparator = scanner.comparator();
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    /**
     * Calculates the next (complex) column at depth 1 by iterating over slices
     * with our decorated key, and buffering Columns a slice at a time.
     */
    @Override
    protected IColumn computeNext()
    {
        try
        {
            return computeNextUnsafe();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private IColumn computeNextUnsafe() throws IOException
    {
        while (buffer.isEmpty() && scanner.get() != null)
        {
            if (comparator.compare(startKey, scanner.get().key, 0) != 0)
                // our decorated key doesn't match this slice
                break;
            
            if (cf == null)
            {
                // build an empty column family using the root metadata of this slice
                cf = scanner.sstable().makeColumnFamily();
                Slice.Metadata meta = scanner.get().meta.get(0);
                cf.delete(meta.localDeletionTime, meta.markedForDeleteAt);
            }

            // buffer any interesting columns in this slice
            if (scanner.sstable().getColumnDepth() == 1)
            {
                // TODO: be optimistic, and peek() on the scanner to see if the whole
                // slice can be buffered without more comparisons

                // standard CF: buffer columns from the slice
                for (Column col : scanner.getColumns())
                {
                    if (comparator.compareAt(col.name(), startKey.name(1), 1) < 0)
                        // column name less than starting key
                        continue;
                    if (comparator.compareAt(finishKey.name(1), col.name(), 1) < 0)
                        // column name greater than finishing key
                        break; // inner loop

                    buffer.add(col);
                }
            }
            else
            {
                // super CF: multiple slices may go into each returned super column
                throw new RuntimeException("Not implemented"); // FIXME
            }

            // move to next slice
            scanner.next();
        }

        // return a buffered column
        return buffer.isEmpty() ? endOfData() : buffer.poll();
    }

    public void close() throws IOException
    {
        scanner.close();
    }
}
