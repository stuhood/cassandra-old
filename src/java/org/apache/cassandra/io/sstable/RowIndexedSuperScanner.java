/**
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
 */

package org.apache.cassandra.io.sstable;

import java.io.*;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.Slice;
import org.apache.cassandra.io.SliceBuffer;

import org.apache.log4j.Logger;

public class RowIndexedSuperScanner extends RowIndexedScanner
{
    private Queue<SuperColumn> supers;

    /**
     * @param reader SSTable to scan.
     * @param bufferSize Buffer size for the file backing the scanner (if supported).
     */
    RowIndexedSuperScanner(RowIndexedReader reader, int bufferSize)
    {
        super(reader, bufferSize);
        supers = new ArrayDeque<SuperColumn>();
    }

    protected void repositionSlice() throws IOException
    {
        // we must always load all super columns so that their metadata can be resolved
        for (SuperColumn col : (List<SuperColumn>)getRawColumns())
            supers.add(col);
    }

    @Override
    protected void clearPosition()
    {
        super.clearPosition();
        supers.clear();
    }

    @Override
    public boolean seekNear(ColumnKey seekKey) throws IOException
    {
        throw new RuntimeException("FIXME: Not implemented"); // FIXME
    }

    @Override
    public boolean seekTo(ColumnKey seekKey) throws IOException
    {
        throw new RuntimeException("FIXME: Not implemented"); // FIXME
    }

    @Override
    public boolean hasNext()
    {
        if (!supers.isEmpty())
            return true;
        return canIncrementChunk();
    }

    @Override
    public SliceBuffer next()
    {
        try
        {
            if (!supers.isEmpty())
                return getSuperSlice();
            if (incrementChunk())
            {
                repositionSlice();
                return getSuperSlice();
            }
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        throw new NoSuchElementException();
    }

    /**
     * Polls the first Slice from the 'supers' queue and filters columns.
     */
    private SliceBuffer getSuperSlice() throws IOException
    {
        // convert the current supercolumn to a slice
        SuperColumn supcol = supers.poll();
        Slice.Metadata supermeta = rowmeta.childWith(supcol.getMarkedForDeleteAt(),
                                                     supcol.getLocalDeletionTime());

        ColumnKey begin = new ColumnKey(rowkey, supcol.name(), ColumnKey.NAME_BEGIN);
        ColumnKey end = new ColumnKey(rowkey, supcol.name(), ColumnKey.NAME_END);

        // drop non-matching subcolumns
        Comparator<byte[]> ccomp = comp.comparatorAt(2);
        List<Column> subcols = new ArrayList<Column>();
        if(filter == null || filter.mightMatchSlice(ccomp, begin.name(2), end.name(2)))
        {
            // might match
            for (IColumn col : supcol.getSubColumns())
            {
                if (filter == null || filter.matchesName(ccomp, col.name()))
                {
                    subcols.add((Column)col);
                }
            }
        }
        return new SliceBuffer(supermeta, begin, end, subcols);
    }
}
