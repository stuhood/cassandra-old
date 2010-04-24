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

package org.apache.cassandra.db;

import java.io.*;
import java.util.*;

import org.apache.cassandra.ASlice;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.TransformingIterator;

import com.google.common.collect.*;

/**
 * Merges sorted non-intersecting ASlices into Rows. Null slices in the input will be ignored, allowing this iterator
 * to be chained with garbage collecting iterators like MergingScanner.
 *
 * NB: Slices represent the metadata of all parents, so ColumnFamilies returned by this iterator will only contain one
 * interesting level of metadata. For super CFs, only supercolumns will contain interesting metadata, and for standard, the CFs themselves will be marked.
 */
public class SliceToRowIterator extends TransformingIterator<ASlice, Row> implements Closeable
{
    private DecoratedKey dk;
    private ColumnFamily cf;

    public SliceToRowIterator(Iterator<ASlice> source, String ksname, String cfname)
    {
        super(source);
        dk = null;
        cf = ColumnFamily.create(ksname, cfname);
    }

    public SliceToRowIterator(Iterator<ASlice> source, SSTableReader reader)
    {
        super(source);
        dk = null;
        cf = reader.makeColumnFamily();
    }

    /**
     * Merges consecutive slices for the same row.
     */
    @Override
    protected boolean transform(ASlice sb)
    {
        if (sb == null)
            // slice was garbage collected earlier
            return true;

        if (dk != null && !dk.equals(sb.begin.dk))
            // slice should go to the next merge phase
            return false;

        dk = sb.begin.dk;
        if (!cf.isSuper())
        {
            cf.delete(sb.meta.localDeletionTime, sb.meta.markedForDeleteAt);
            for (Column col : sb.columns())
                cf.addColumn(col);
            return true;
        }

        // super cf
        byte[] scname = sb.begin.name(1);
        SuperColumn sc = (SuperColumn)cf.getColumn(scname);
        if (sc == null)
        {
            sc = new SuperColumn(scname, cf.getSubComparator());
            sc.markForDeleteAt(sb.meta.localDeletionTime, sb.meta.markedForDeleteAt);
            cf.addColumn(sc);
        }
        for (Column col : sb.columns())
            sc.addColumn(col);
        return true;
    }

    @Override
    protected boolean complete()
    {
        return dk != null;
    }

    @Override
    protected Row transformed()
    {
        Row row = new Row(dk, cf);
        dk = null;
        cf = cf.cloneMeShallow();
        return row;
    }

    public void close() throws IOException
    {
        if (source instanceof Closeable)
            ((Closeable)source).close();
    }
}
