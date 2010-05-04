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
    private final IKeyFilter keyfilter;
    private final INameFilter[] filters;

    /**
     * An identity query filter of the given depth.
     */
    protected QueryFilter(String cfname, int depth)
    {
        this.cfname = cfname;
        this.keyfilter = KeyIdentityFilter.get();
        this.filters = new INameFilter[depth];
        for (int i = 0; i < depth; i++)
            this.filters[i] = NameIdentityFilter.get();
    }

    protected QueryFilter(String cfname, IKeyFilter keyfilter, INameFilter... filters)
    {
        this.cfname = cfname;
        this.keyfilter = keyfilter;
        this.filters = filters;
    }

    /**
     * @return The IKeyFilter for this query.
     */
    public IKeyFilter keyFilter()
    {
        return keyfilter;
    }

    /**
     * @return The INameFilter at the given depth.
     */
    public INameFilter nameFilter(int depth)
    {
        return filters[depth-1];
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
        return filters[0].getMemtableColumnIterator(cf, key, comparator);
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
                if (filters.length == 2)
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

        filters[0].collectReducedColumns(returnCF, reduced, gcBefore);
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
        if("Super".equals(DatabaseDescriptor.getColumnFamilyType(ksname, cfname)))
            return new QueryFilter(cfname, 2);
        return new QueryFilter(cfname, 1);
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
     * @return A copy of the name filters for this QueryFilter, with the given depth set to the given filter.
     */
    private INameFilter[] copyAndAdd(int depth, INameFilter filt)
    {
        INameFilter[] filts = Arrays.copyOf(filters, filters.length);
        filts[depth - 1] = filt;
        return filts;
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
        return new QueryFilter(cfname, keyfilter, copyAndAdd(depth, new NameSliceFilter(start, finish, bitmasks, reversed, limit)));
    }

    /**
     * @return Creates a copy of this QueryFilter with a name list filter at the given depth.
     * @param depth depth to filter names at
     * @param columns the column names to restrict the results to
     */
    public QueryFilter forNames(int depth, SortedSet<byte[]> columns)
    {
        return new QueryFilter(cfname, keyfilter, copyAndAdd(depth, new NameListFilter(columns)));
    }

    /**
     * convenience method for creating a name filter matching a single column
     */
    public QueryFilter forName(int depth, byte[] column)
    {
        return new QueryFilter(cfname, keyfilter, copyAndAdd(depth, new NameListFilter(column)));
    }

    /**
     * @return A filter matching the given key.
     */
    public QueryFilter forKey(DecoratedKey key)
    {
        // clone this query with the added key filter
        return new QueryFilter(cfname, new KeyMatchFilter(key), Arrays.copyOf(filters, filters.length));
    }

    /**
     * @return A filter matching keys in any of the given Ranges.
     */
    public QueryFilter forRange(AbstractBounds range)
    {
        // clone this query with the added key filter
        return new QueryFilter(cfname, new KeyRangeFilter(range), Arrays.copyOf(filters, filters.length));
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
        for (INameFilter filt : filters)
            buff.append(", ").append(filt);
        return buff.append("]>").toString();
    }
}
