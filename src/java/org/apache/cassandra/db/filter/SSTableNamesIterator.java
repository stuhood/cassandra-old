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

import java.io.IOError;
import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class SSTableNamesIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    private ColumnFamily cf;
    private final SSTableScanner scanner;
    private final ColumnKey.Comparator comparator;
    private final Named.Comparator ncomparator;

    private final PeekingIterator<ColumnKey> keys;
    private final Queue<IColumn> buffer;

    /**
     * TODO: Despite the fact that this interface takes ColumnKeys, the columns it
     * returns are not labeled with the row key, so it would be unwise to use it
     * to collect names from more than one row.
     */
    public SSTableNamesIterator(SSTableReader ssTable, SortedSet<ColumnKey> keys) throws IOException
    {
        assert keys != null;

        this.keys = Iterators.peekingIterator(keys.iterator());
        buffer = new ArrayDeque<IColumn>();

        scanner = ssTable.getScanner(DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
        comparator = scanner.comparator();
        ncomparator = new Named.Comparator(comparator);
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    /**
     * Seeks to slices that might contain keys we're looking for, and iterates
     * through them collecting matching keys.
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
        while (buffer.isEmpty() && keys.hasNext())
        {
            if (!scanner.seekTo(keys.peek()))
            {
                // filter or index determined that this key is not in this sstable
                keys.next();
                continue;
            }
            if (comparator.compare(keys.peek(), scanner.get().key, 0) != 0)
                // no slices with our decorated key
                break;

            if (cf == null)
            {
                // build an empty column family using the root metadata of this slice
                cf = scanner.sstable().makeColumnFamily();
                Slice.Metadata meta = scanner.get().meta.get(0);
                cf.delete(meta.localDeletionTime, meta.markedForDeleteAt);
            }

            // positioned at a slice that might contain some of our keys
            if (scanner.sstable().getColumnDepth() == 1)
            {
                // standard CF
                Slice slice = scanner.get();
                List<Column> slicecols = scanner.getColumns();
                while (keys.hasNext())
                {
                    // search for current key in the slice
                    int idx = Collections.binarySearch(slicecols, keys.peek(),
                                                       ncomparator);
                    if (0 <= idx)
                    {
                        // found key
                        buffer.add(slicecols.get(idx));
                        keys.next();
                    }
                    else
                    {
                        // key not in current slice
                        if (ncomparator.compare(slice.end, keys.peek()) <= 0)
                            // possibly in another slice
                            break; // inner
                        else
                            // doesn't exist
                            keys.next();
                    }
                }
            }
            else
            {
                // super CF: multiple slices may go into each returned super column
                throw new RuntimeException("Not implemented"); // FIXME
            }
        }
        
        return buffer.isEmpty() ? endOfData() : buffer.poll();
    }

    @Override
    public void close() throws IOException
    {
        scanner.close();
    }
}
