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

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Slice;

import org.apache.cassandra.db.*;

import org.apache.log4j.Logger;

public class RowIndexedSuperScanner extends RowIndexedScanner
{
    // slices which have been loaded from disk
    private Queue<ASlice> slices;

    /**
     * @param reader SSTable to scan.
     * @param bufferSize Buffer size for the file backing the scanner (if supported).
     */
    RowIndexedSuperScanner(RowIndexedReader reader, int bufferSize)
    {
        super(reader, bufferSize);
        slices = new ArrayDeque<ASlice>();
    }

    /**
     * Reads all columns for this chunk from disk: this is a lazy approach, to make it simpler to deal with tombstone
     * rows, which have zero chunks (I hate supercolumns).
     */
    protected void loadChunk() throws IOException
    {
        if (rowIsTombstone())
        {
            // single tombstone slice for the row
            ColumnKey begin = new ColumnKey(rowkey, ColumnKey.NAME_BEGIN, ColumnKey.NAME_BEGIN);
            ColumnKey end = new ColumnKey(rowkey, ColumnKey.NAME_END, ColumnKey.NAME_END);
            slices.add(new Slice(rowmeta, begin, end, Collections.<Column>emptyList()));
            return;
        }

        // load each super column as a slice so that metadata can be resolved
        for (SuperColumn supcol : (List<SuperColumn>)getRawColumns())
        {
            // convert the current supercolumn to a slice
            ASlice.Metadata supermeta = rowmeta.max(supcol.getMarkedForDeleteAt(),
                                                    supcol.getLocalDeletionTime());

            ColumnKey begin = new ColumnKey(rowkey, supcol.name(), ColumnKey.NAME_BEGIN);
            ColumnKey end = new ColumnKey(rowkey, supcol.name(), ColumnKey.NAME_END);

            // drop non-matching subcolumns
            List<Column> subcols = new ArrayList<Column>();
            for (IColumn col : supcol.getSubColumns())
                    subcols.add((Column)col);
            slices.add(new Slice(supermeta, begin, end, subcols));
        }
    }

    @Override
    protected void clearPosition()
    {
        super.clearPosition();
        slices.clear();
    }

    @Override
    public boolean seekNear(ColumnKey seekKey) throws IOException
    {
        slices.clear();
        if (!super.seekNear(seekKey))
            return false;
        if (incrementChunk())
            loadChunk();

        // skip slices while < seekKey
        while (!slices.isEmpty() && comparator().compare(slices.peek().end, seekKey) < 0)
            slices.poll();
        return !slices.isEmpty();
    }

    @Override
    public boolean seekTo(ColumnKey seekKey) throws IOException
    {
        throw new RuntimeException("FIXME: Not implemented"); // FIXME
    }

    @Override
    public boolean hasNext()
    {
        if (!slices.isEmpty())
            return true;
        return super.hasNext();
    }

    @Override
    public ASlice next()
    {
        try
        {
            if (!slices.isEmpty())
                return slices.poll();
            if (incrementChunk())
            {
                loadChunk();
                return slices.poll();
            }
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        throw new NoSuchElementException();
    }
}
