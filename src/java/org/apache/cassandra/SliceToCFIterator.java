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

package org.apache.cassandra;

import java.io.*;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.Slice;
import org.apache.cassandra.ASlice;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ReducingIterator;

import com.google.common.collect.*;

/**
 * Merges sorted ASlices into Pairs of DK and CF.
 *
 * FIXME: This is here temporarily as we port callers to the Slice API.
 */
@Deprecated
public class SliceToCFIterator extends ReducingIterator<ASlice, Pair<DecoratedKey,ColumnFamily>>
{
    private DecoratedKey dk;
    private ColumnFamily cf;

    public SliceToCFIterator(String ksname, String cfname, Iterator<ASlice> source)
    {
        super(source);
        dk = null;
        cf = ColumnFamily.create(ksname, cfname);
    }

    /**
     * @return True if the Slices are from the same row.
     */
    @Override
    protected boolean isEqual(ASlice sb1, ASlice sb2)
    {
        return sb1.begin.dk.compareTo(sb2.begin.dk) == 0;
    }
    
    @Override
    public void reduce(ASlice sb)
    {
        dk = sb.begin.dk;
        cf.delete(sb.meta.get(0).localDeletionTime, sb.meta.get(0).markedForDeleteAt);
        if (!cf.isSuper())
        {
            for (Column col : sb.columns())
                cf.addColumn(col);
            return;
        }

        // super cf
        byte[] scname = sb.begin.name(1);
        SuperColumn sc = (SuperColumn)cf.getColumn(scname);
        if (sc == null)
        {
            sc = new SuperColumn(scname, cf.getSubComparator());
            sc.markForDeleteAt(sb.meta.get(1).localDeletionTime, sb.meta.get(1).markedForDeleteAt);
            cf.addColumn(sc);
        }
        for (Column col : sb.columns())
            sc.addColumn(col);
    }

    @Override
    protected Pair<DecoratedKey, ColumnFamily> getReduced()
    {
        ColumnFamily toret = cf;
        cf = toret.cloneMeShallow();
        return new Pair(dk, toret);
    }
}
