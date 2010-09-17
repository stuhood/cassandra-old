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

package org.apache.cassandra.db.secindex;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.ReducingIterator;

/**
 * An iterator over rows in a Memtable which resolves rows and returns keys for those which match the filter.
 */
public class MemtableFilteringIterator extends ReducingIterator<Entry<DecoratedKey,ColumnFamily>,DecoratedKey> implements CloseableIterator<DecoratedKey>
{
    private final List<IndexExpression> exprs;
    private ColumnFamily current = null;
    private DecoratedKey currentKey = null;

    public MemtableFilteringIterator(IndexExpression expr, List<Iterator<Entry<DecoratedKey,ColumnFamily>>> sources)
    {
        super(sources.size() > 1 ? getCollatingIterator(sources) : sources.get(0));
        this.exprs = Collections.singletonList(expr);
    }

    private static CollatingIterator getCollatingIterator(List<Iterator<Entry<DecoratedKey,ColumnFamily>>> sources)
    {
        CollatingIterator citer = new CollatingIterator(ROW_COMPARATOR);
        for (Iterator<Entry<DecoratedKey,ColumnFamily>> source : sources)
            citer.addIterator(source);
        return citer;
    }
    
    @Override
    protected boolean isEqual(Entry<DecoratedKey,ColumnFamily> o1, Entry<DecoratedKey,ColumnFamily> o2)
    {
        return o1.getKey().equals(o2.getKey());
    }

    @Override
    public void reduce(Entry<DecoratedKey,ColumnFamily> inc)
    {
        currentKey = inc.getKey();
        if (current == null)
            current = inc.getValue().cloneMe();
        else
        {
            current.delete(inc.getValue());
            current.addAll(inc.getValue());
        }
    }

    @Override
    protected DecoratedKey getReduced()
    {
        if (!SecondaryIndex.satisfies(current, exprs))
            currentKey = null;
        current.clear();
        return currentKey;
    }

    public void close() { /* pass */ }

    public static final Comparator<Entry<DecoratedKey,ColumnFamily>> ROW_COMPARATOR = new Comparator<Entry<DecoratedKey,ColumnFamily>>()
    {
        public int compare(Entry<DecoratedKey,ColumnFamily> o1, Entry<DecoratedKey,ColumnFamily> o2)
        {
            return o1.getKey().compareTo(o2.getKey());
        }
    };
}
