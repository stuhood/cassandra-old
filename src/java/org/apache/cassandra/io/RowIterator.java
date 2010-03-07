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

package org.apache.cassandra.io;

import java.io.*;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IteratingRow;
import org.apache.cassandra.io.Slice;
import org.apache.cassandra.io.SliceBuffer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ReducingIterator;

import com.google.common.collect.*;

/**
 * Merges non-intersecting SliceBuffers into an IteratingRow.
 *
 * FIXME: This is here temporarily as we port callers to the Slice API.
 */
@Deprecated
public class RowIterator extends ReducingIterator<SliceBuffer, IteratingRow> implements PeekingIterator<IteratingRow>, Closeable
{
    public final SSTableReader reader;
    private final boolean isSuper;
    private DecoratedKey dk;
    private ColumnFamily cf;

    public RowIterator(Iterator<SliceBuffer> source, SSTableReader reader)
    {
        super(source);
        this.reader = reader;
        isSuper = reader.getColumnDepth() == 2;

        dk = null;
        cf = reader.makeColumnFamily();
    }

    @Override
    protected boolean isEqual(SliceBuffer sb1, SliceBuffer sb2)
    {
        return sb1.begin.dk.compareTo(sb2.begin.dk) == 0;
    }
    
    @Override
    public void reduce(SliceBuffer sb)
    {
        dk = sb.begin.dk;
        cf.delete(sb.meta.get(0).localDeletionTime, sb.meta.get(0).markedForDeleteAt);
        if (!isSuper)
        {
            for (Column col : sb.realized())
                cf.addColumn(col);
            return;
        }

        // super cf
        byte[] scname = sb.begin.name(1);
        SuperColumn sc = (SuperColumn)cf.getColumn(scname);
        if (sc == null)
        {
            sc = new SuperColumn(scname, reader.getComparator().typeAt(2));
            sc.markForDeleteAt(sb.meta.get(1).localDeletionTime, sb.meta.get(1).markedForDeleteAt);
            cf.addColumn(sc);
        }
        for (Column col : sb.realized())
            sc.addColumn(col);
    }

    @Override
    protected IteratingRow getReduced()
    {
        ColumnFamily toret = cf;
        cf = reader.makeColumnFamily();
        return new IteratingRow(dk, toret);
    }

    public void close() throws IOException
    {
        if (source instanceof Closeable)
            ((Closeable)source).close();
    }
}
