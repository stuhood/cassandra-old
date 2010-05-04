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
    private final IFilter<DecoratedKey> keyfilter;
    private final List<IFilter<byte[]>> filters;

    /**
     * An identity query filter of the given depth.
     */
    protected QueryFilter(String cfname, ColumnKey.Comparator comp)
    {
        this.cfname = cfname;
        this.comp = comp;
        this.keyfilter = KeyIdentityFilter.get();
        int depth = comp.columnDepth();
        this.filters = new ArrayList<IFilter<byte[]>>(depth);
        for (int i = 1; i <= depth; i++)
            this.filters.add(new NameIdentityFilter(comp.comparatorAt(i)));
    }

    protected QueryFilter(String cfname, ColumnKey.Comparator comp, IFilter<DecoratedKey> keyfilter, List<IFilter<byte[]>> filters)
    {
        this.cfname = cfname;
        this.comp = comp;
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
     * @return A filtered and limited scanner.
     */
    public Scanner filter(SeekableScanner scanner)
    {
        Scanner filtered = new FilteredScanner(scanner, this);
        return filtered;
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
     * @return True if the slice represented by the given ColumnKeys might contain matching columns.
     */
    public boolean matches(ColumnKey begin, ColumnKey end)
    {
        if (!keyFilter().matchesBetween(begin.dk, end.dk))
            return false;
        for (int i = 1; i <= comp.columnDepth(); i++)
        {
            if (!nameFilter(i).matchesBetween(begin.name(i), end.name(i)))
                return false;
        }
        return true;
    }

    public IColumnIterator getMemtableColumnIterator(Memtable memtable, AbstractType comparator)
    {
        ColumnFamily cf = memtable.getColumnFamily(key());
        if (cf == null)
            return null;
        return getMemtableColumnIterator(cf, key(), comparator);
    }

    public IColumnIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key, AbstractType comparator)
    {
        assert cf != null;
        return filters.get(0).getMemtableColumnIterator(cf, key, comparator);
    }

    public static Comparator<IColumn> getColumnComparator(final AbstractType comparator)
    {
        return new Comparator<IColumn>()
        {
            public int compare(IColumn c1, IColumn c2)
            {
                return comparator.compare(c1.name(), c2.name());
            }
        };
    }
    
    public void collectCollatedColumns(final ColumnFamily returnCF, Iterator<IColumn> collatedColumns, final int gcBefore)
    {
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        ReducingIterator<IColumn, IColumn> reduced = new ReducingIterator<IColumn, IColumn>(collatedColumns)
        {
            ColumnFamily curCF = returnCF.cloneMeShallow();

            protected boolean isEqual(IColumn o1, IColumn o2)
            {
                return Arrays.equals(o1.name(), o2.name());
            }

            public void reduce(IColumn current)
            {
                curCF.addColumn(current);
            }

            protected IColumn getReduced()
            {
                IColumn c = curCF.getSortedColumns().iterator().next();
                if (filters.size() == 2)
                {
                    // filterSuperColumn only looks at immediate parent (the supercolumn) when determining if a subcolumn
                    // is still live, i.e., not shadowed by the parent's tombstone.  so, bump it up temporarily to the tombstone
                    // time of the cf, if that is greater.
                    long deletedAt = c.getMarkedForDeleteAt();
                    if (returnCF.getMarkedForDeleteAt() > deletedAt)
                        ((SuperColumn)c).markForDeleteAt(c.getLocalDeletionTime(), returnCF.getMarkedForDeleteAt());

                    // subcolumn filter
                    c = nameFilter(2).filterSuperColumn((SuperColumn)c, gcBefore);
                    ((SuperColumn)c).markForDeleteAt(c.getLocalDeletionTime(), deletedAt); // reset sc tombstone time to what it should be
                }
                curCF.clear();
                return c;
            }
        };

        filters.get(0).collectReducedColumns(returnCF, reduced, gcBefore);
    }

    public String getColumnFamilyName()
    {
        return cfname;
    }

    public static boolean isRelevant(IColumn column, IColumnContainer container, int gcBefore)
    {
        // the column itself must be not gc-able (it is live, or a still relevant tombstone, or has live subcolumns), (1)
        // and if its container is deleted, the column must be changed more recently than the container tombstone (2)
        // (since otherwise, the only thing repair cares about is the container tombstone)
        long maxChange = column.mostRecentLiveChangeAt();
        return (!column.isMarkedForDelete() || column.getLocalDeletionTime() > gcBefore || maxChange > column.getMarkedForDeleteAt()) // (1)
               && (!container.isMarkedForDelete() || maxChange > container.getMarkedForDeleteAt()); // (2)
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
     * @return Creates a copy of this QueryFilter with a slice filter at the given depth.
     * @param depth depth to slice at
     * @param start column to start slice at, inclusive; empty for "the first column"
     * @param finish column to stop slice at, inclusive; empty for "the last column"
     * @param bitmasks we should probably remove this
     * @param reversed true to start with the largest column (as determined by configured sort order) instead of smallest
     * @param limit maximum number of non-deleted columns to return
     */
    public QueryFilter forSlice(int depth, byte[] start, byte[] finish, List<byte[]> bitmasks, boolean reversed, int limit)
    {
        return new QueryFilter(this, depth, new NameSliceFilter(comp.comparatorAt(depth), start, finish, bitmasks, reversed, limit));
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
        return new QueryFilter(cfname, comp, new KeyMatchFilter(key), filters);
    }

    /**
     * @return A filter matching keys in any of the given Ranges.
     */
    public QueryFilter forRange(AbstractBounds range)
    {
        // clone this query with the added key filter
        return new QueryFilter(cfname, comp, new KeyRangeFilter(range), filters);
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
