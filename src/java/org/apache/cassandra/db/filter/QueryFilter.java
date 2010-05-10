package org.apache.cassandra.db.filter;
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


import java.util.*;

import org.apache.cassandra.Scanner;
import org.apache.cassandra.SeekableScanner;

import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;

/**
 * Describes a simple query plan for ranges of columns, with a filter per column parent.
 *
 * TODO: should support the concept of a reversed query natively.
 */
public final class QueryFilter
{
    private final String cfname;
    private final ColumnKey.Comparator comp;
    private final long limit;
    private final IFilter<DecoratedKey> keyfilter;
    private final List<IFilter<byte[]>> filters;

    /**
     * An identity query filter of the given depth.
     */
    protected QueryFilter(String cfname, ColumnKey.Comparator comp)
    {
        this.cfname = cfname;
        this.comp = comp;
        this.limit = Long.MAX_VALUE;
        this.keyfilter = KeyIdentityFilter.get();
        int depth = comp.columnDepth();
        this.filters = new ArrayList<IFilter<byte[]>>(depth);
        for (int i = 1; i <= depth; i++)
            this.filters.add(NameIdentityFilter.get());
    }

    protected QueryFilter(String cfname, ColumnKey.Comparator comp, long limit, IFilter<DecoratedKey> keyfilter, List<IFilter<byte[]>> filters)
    {
        this.cfname = cfname;
        this.comp = comp;
        this.limit = limit;
        this.keyfilter = keyfilter;
        this.filters = filters;
    }

    /**
     * Copy constructor for adding a name filter.
     */
    protected QueryFilter(QueryFilter tocopy, int depth, IFilter<byte[]> newfilter)
    {
        this.cfname = tocopy.cfname;
        this.comp = tocopy.comp;
        this.limit = tocopy.limit;
        this.keyfilter = tocopy.keyfilter;
        // clone name filters, and replace depth with newfilter
        this.filters = new ArrayList(tocopy.filters);
        this.filters.set(depth - 1, newfilter);
    }

    /**
     * @return The Key*Filter for this query.
     */
    public IFilter<DecoratedKey> keyFilter()
    {
        return keyfilter;
    }

    /**
     * @return The IFilter at the given depth.
     */
    public IFilter<byte[]> nameFilter(int depth)
    {
        return filters.get(depth-1);
    }

    /**
     * Wraps filtering for this QueryFilter around the given scanner.
     * @param scanner Scanner to filter.
     * @return A filtered scanner.
     */
    public Scanner filter(SeekableScanner scanner)
    {
        return new FilteredScanner(scanner, this);
    }

    /**
     * Wraps limiting for this QueryFilter around the given scanner. If the limit is unset, this is a noop.
     *
     * @param scanner Scanner to limit.
     * @return A limited scanner.
     */
    public Scanner limit(Scanner scanner)
    {
        if (limit < Long.MAX_VALUE)
            return new LimitingScanner(scanner, limit);
        return scanner;
    }

    /**
     * FIXME: transitional: remove once Memtables are using the Scanner API
     */
    @Deprecated
    public DecoratedKey key()
    {
        assert keyfilter instanceof KeyMatchFilter;
        return ((KeyMatchFilter)keyfilter).key;
    }

    /**
     * @return A MatchResult indicating whether the Slice between the given keys may match the filter, and where
     * the next possible match is.
     */
    public MatchResult<ColumnKey> matches(ColumnKey begin, ColumnKey end)
    {
        if (!keyFilter().matchesBetween(begin.dk, end.dk).matched)
            return MatchResult.NOMATCH_CONT;
        for (int i = 1; i <= comp.columnDepth(); i++)
        {
            if (!nameFilter(i).matchesBetween(begin.name(i), end.name(i)).matched)
                return MatchResult.NOMATCH_CONT;
        }
        return MatchResult.MATCH_CONT;
    }

    public String getColumnFamilyName()
    {
        return cfname;
    }

    /**
     * The beginning of the chain for building a QueryFilter.
     * @return An identity QueryFilter for the given CF.
     */
    public static QueryFilter on(ColumnFamilyStore cfs)
    {
        return on(cfs.table_, cfs.columnFamily_);
    }

    /**
     * The beginning of the chain for building a QueryFilter.
     * @return An identity QueryFilter for the given CF.
     */
    public static QueryFilter on(String ksname, String cfname)
    {
        ColumnKey.Comparator comp = ColumnKey.getComparator(ksname, cfname);
        return new QueryFilter(cfname, comp);
    }

    /**
     * TODO: Replace QueryPath with an arbitrarily nested filter that mirrors QueryFilter.
     */
    public static QueryFilter on(String ksname, QueryPath path)
    {
        QueryFilter qf = on(ksname, path.columnFamilyName);
        return path.superColumnName != null ? qf.forName(1, path.superColumnName) : qf;
    }

    /**
     * @param newlim maximum number of non-deleted columns to return
     * @return Creates a copy of this QueryFilter that limits the number of returned columns.
     */
    public QueryFilter limitedTo(long newlim)
    {
        return new QueryFilter(cfname, comp, newlim, keyfilter, filters);
    }

    /**
     * @return Creates a copy of this QueryFilter with a slice filter at the given depth.
     * @param depth depth to slice at
     * @param start column to start slice at, inclusive; empty for "the first column"
     * @param finish column to stop slice at, inclusive; empty for "the last column"
     * @param bitmasks we should probably remove this
     * @param reversed true to start with the largest column (as determined by configured sort order) instead of smallest
     */
    public QueryFilter forSlice(int depth, byte[] start, byte[] finish, List<byte[]> bitmasks, boolean reversed)
    {
        return new QueryFilter(this, depth, new NameSliceFilter(comp.comparatorAt(depth), start, finish, bitmasks, reversed));
    }

    /**
     * @return Creates a copy of this QueryFilter with a name list filter at the given depth.
     * @param depth depth to filter names at
     * @param columns the column names to restrict the results to
     */
    public QueryFilter forNames(int depth, Collection<byte[]> columns)
    {
        return new QueryFilter(this, depth, new NameListFilter(comp.comparatorAt(depth), columns));
    }

    /**
     * convenience method for creating a name filter matching a single column
     */
    public QueryFilter forName(int depth, byte[] column)
    {
        return new QueryFilter(this, depth, new NameMatchFilter(comp.comparatorAt(depth), column));
    }

    /**
     * @return A filter matching the given key.
     */
    public QueryFilter forKey(DecoratedKey key)
    {
        // clone this query with the added key filter
        return new QueryFilter(cfname, comp, limit, new KeyMatchFilter(key), filters);
    }

    /**
     * @return A filter matching keys in any of the given Ranges.
     */
    public QueryFilter forRange(AbstractBounds range)
    {
        // clone this query with the added key filter
        return new QueryFilter(cfname, comp, limit, new KeyRangeFilter(range), filters);
    }

    /**
     * @return A filter matching keys in any of the given Ranges.
     */
    public QueryFilter forRanges(Collection<Range> ranges)
    {
        // FIXME: temporarily assuming one un-wrapped range per anticompaction until we support compound filters.
        assert ranges == null || ranges.size() == 1 : "FIXME: Not implemented!";
        if (ranges == null)
            return this;
        List<AbstractBounds> boundslist = ranges.iterator().next().unwrap();
        assert boundslist.size() == 1;
        AbstractBounds bounds = boundslist.iterator().next();

        return forRange(bounds);
    }

    @Override
    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<QueryFilter [").append(keyfilter);
        for (IFilter<byte[]> filt : filters)
            buff.append(", ").append(filt);
        return buff.append("]>").toString();
    }
}
