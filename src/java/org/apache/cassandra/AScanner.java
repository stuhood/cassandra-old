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

import java.util.*;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Slice;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryFilter;

public abstract class AScanner implements SeekableScanner
{
    protected final ColumnKey.Comparator comparator;
    // optional pushed down filter to apply to column names in returned slices
    protected IFilter<byte[]> filter = null;

    public AScanner(ColumnKey.Comparator comparator)
    {
        this.comparator = comparator;
    }

    public ColumnKey.Comparator comparator()
    {
        return comparator;
    }

    public void pushdownFilter(IFilter<byte[]> filter)
    {
        this.filter = filter;
    }

    public long getBytesRemaining()
    {
        return 1;
    }

    @Override
    public boolean seekNear(DecoratedKey seekKey)
    {
        return seekNear(new ColumnKey(seekKey, comparator.columnDepth()));
    }

    @Override
    public boolean seekNear(ColumnKey seekKey)
    {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean seekTo(DecoratedKey seekKey)
    {
        return seekTo(new ColumnKey(seekKey, comparator.columnDepth()));
    }

    @Override
    public boolean seekTo(ColumnKey seekKey)
    {
        throw new RuntimeException("Not implemented");
    }

    /**
     * @return A new or identical slice with filtered columns removed.
     */
    protected ASlice filter(ASlice slice)
    {
        if (filter == null)
            return slice;

        List<Column> filtered = new ArrayList<Column>();
        for (Column col : slice.columns())
            if (filter.matches(col.name()))
                filtered.add(col);

        if (filtered.size() == slice.count())
            return slice;

        return new Slice(slice.meta, slice.begin, slice.end, filtered);
    }

    public void remove()
    {
        throw new RuntimeException("Not supported.");
    }

    public void close() { }
}
