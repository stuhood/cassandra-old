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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.collections.iterators.ReverseListIterator;
import org.apache.commons.collections.IteratorUtils;

import com.google.common.collect.Collections2;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

public class NameSliceFilter implements INameFilter
{
    private static Logger logger = LoggerFactory.getLogger(NameSliceFilter.class);

    public final byte[] start;
    public final byte[] finish;
    public final List<byte[]> bitmasks;
    public final boolean reversed;
    public final int count;

    public NameSliceFilter(byte[] start, byte[] finish, List<byte[]> bitmasks, boolean reversed, int count)
    {
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.count = count;
        this.bitmasks = bitmasks;
    }

    public IColumnIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key, AbstractType comparator)
    {
        return Memtable.getSliceIterator(key, cf, this, comparator);
    }

    @Override
    public boolean mightMatchSlice(Comparator<byte[]> comp, byte[] begin, byte[] end)
    {
        // non-wrapping intersection
        // TODO: replace finish.length check with ColumnKey.NAME_END
        return comp.compare(start, end) <= 0 && (finish.length == 0 || comp.compare(begin, finish) <= 0);
    }

    @Override
    public boolean matchesName(Comparator<byte[]> comp, byte[] name)
    {
        if (bitmasks != null && !bitmasks.isEmpty() && !matchesBitmasks(name))
            return false;

        // contains
        // TODO: replace finish.length check with ColumnKey.NAME_END when all
        // components are using Slices
        return comp.compare(start, name) <= 0 && (finish.length == 0 || comp.compare(name, finish) <= 0);
    }
    
    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        // we clone shallow, then add, under the theory that generally we're interested in a relatively small number of subcolumns.
        // this may be a poor assumption.
        SuperColumn scFiltered = superColumn.cloneMeShallow();
        Iterator<IColumn> subcolumns;
        if (reversed)
        {
            List<IColumn> columnsAsList = new ArrayList<IColumn>(superColumn.getSubColumns());
            subcolumns = new ReverseListIterator(columnsAsList);
        }
        else
        {
            subcolumns = superColumn.getSubColumns().iterator();
        }

        // now apply the predicate
        if (bitmasks != null && !bitmasks.isEmpty())
        {
            subcolumns = Iterators.filter(subcolumns, getBitmaskMatchColumnPredicate());
        }

        // iterate until we get to the "real" start column
        Comparator<byte[]> comparator = reversed ? superColumn.getComparator().getReverseComparator() : superColumn.getComparator();
        while (subcolumns.hasNext())
        {
            IColumn column = subcolumns.next();
            if (comparator.compare(column.name(), start) >= 0)
            {
                subcolumns = IteratorUtils.chainedIterator(IteratorUtils.singletonIterator(column), subcolumns);
                break;
            }
        }
        // subcolumns is either empty now, or has been redefined in the loop above.  either is ok.
        collectReducedColumns(scFiltered, subcolumns, gcBefore);
        return scFiltered;
    }

    public Comparator<IColumn> getColumnComparator(AbstractType comparator)
    {
        return reversed ? new ReverseComparator(QueryFilter.getColumnComparator(comparator)) : QueryFilter.getColumnComparator(comparator);
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        int liveColumns = 0;
        AbstractType comparator = container.getComparator();

        while (reducedColumns.hasNext())
        {
            if (liveColumns >= count)
                break;

            IColumn column = reducedColumns.next();
            if (logger.isDebugEnabled())
                logger.debug(String.format("collecting %s of %s: %s",
                                           liveColumns, count, column.getString(comparator)));

            if (finish.length > 0
                && ((!reversed && comparator.compare(column.name(), finish) > 0))
                    || (reversed && comparator.compare(column.name(), finish) < 0))
                break;

            // only count live columns towards the `count` criteria
            if (!column.isMarkedForDelete()
                && (!container.isMarkedForDelete()
                    || column.mostRecentLiveChangeAt() > container.getMarkedForDeleteAt()))
            {
                liveColumns++;
            }

            // but we need to add all non-gc-able columns to the result for read repair:
            if (QueryFilter.isRelevant(column, container, gcBefore))
                container.addColumn(column);
        }
    }

    public Collection<IColumn> applyPredicate(Collection<IColumn> columns)
    {
        if (bitmasks == null || bitmasks.isEmpty())
            return columns;

        return Collections2.filter(columns, getBitmaskMatchColumnPredicate());
    }

    @SuppressWarnings("unchecked")
    private Predicate<IColumn> getBitmaskMatchColumnPredicate()
    {
        Predicate<IColumn>[] predicates = new Predicate[bitmasks.size()];
        for (int i = 0; i < bitmasks.size(); i++)
        {
            final byte[] bitmask = bitmasks.get(i);
            predicates[i] = new Predicate<IColumn>()
            {
                public boolean apply(IColumn col)
                {
                    return matchesBitmask(bitmask, col.name());
                }
            };
        }
        return Predicates.or(predicates);
    }

    public boolean matchesBitmasks(byte[] name)
    {
        for (byte[] bitmask : bitmasks)
            if (matchesBitmask(bitmask, name))
                return true;
        return false;
    }

    public static boolean matchesBitmask(byte[] bitmask, byte[] name)
    {
        assert name != null;
        assert bitmask != null;

        int len = Math.min(bitmask.length, name.length);

        for (int i = 0; i < len; i++)
        {
            if ((bitmask[i] & name[i]) == 0)
            {
                return false;
            }
        }

        return true;
    }
}
