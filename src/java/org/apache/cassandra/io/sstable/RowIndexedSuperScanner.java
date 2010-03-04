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
    RowIndexedSuperScanner(RowIndexedReader reader, int bufferSize) throws IOException
    {
        super(reader, bufferSize);
        supers = new ArrayDeque<SuperColumn>();
    }

    @Override
    protected void repositionRow(long offset) throws IOException
    {
        super.repositionRow(offset);
        supers.clear();
        repositionSlice();
    }

    protected void repositionSlice() throws IOException
    {
        for (IColumn col : getRawColumns())
            supers.add((SuperColumn)col);
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
    public boolean next() throws IOException
    {
        if (supers.poll() != null && supers.peek() != null)
            return true;
        if (super.next())
        {
            repositionSlice();
            return true;
        }
        return false;
    }

    @Override
    public Slice get()
    {
        if (supers.isEmpty())
            return null;
        
        // convert the current supercolumn to a slice
        SuperColumn supcol = supers.peek();
        Slice.Metadata supermeta = rowmeta.childWith(supcol.getMarkedForDeleteAt(),
                                                     supcol.getLocalDeletionTime());
        return new Slice(supermeta,
                         new ColumnKey(rowkey, supcol.name(), ColumnKey.NAME_BEGIN),
                         new ColumnKey(rowkey, supcol.name(), ColumnKey.NAME_END));
    }

    @Override
    public List<Column> getColumns() throws IOException
    {
        if (supers.isEmpty())
            return null;
        
        List<Column> subcols = new ArrayList<Column>();
        for (IColumn subcol : supers.peek().getSubColumns())
            subcols.add((Column)subcol);
        return subcols;
    }
}
