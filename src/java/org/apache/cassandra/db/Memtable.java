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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.SliceMergingIterator;
import org.apache.cassandra.SliceToCFIterator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.StorageService;

import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Maintains a mapping from column parents (aka, 'rows' for standard cfs, and 'supercolumns' for super cfs) to lists of
 * Slices with those parents.
 */
public class Memtable implements Comparable<Memtable>, IFlushable
{
    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);

    private boolean isFrozen;

    private final int THRESHOLD = DatabaseDescriptor.getMemtableThroughput() * 1024*1024; // not static since we might want to change at runtime
    private final int THRESHOLD_COUNT = (int)(DatabaseDescriptor.getMemtableOperations() * 1024*1024);

    private final AtomicInteger currentThroughput = new AtomicInteger(0);
    private final AtomicInteger currentOperations = new AtomicInteger(0);

    private final long creationTime;
    private final ConcurrentNavigableMap<ColumnKey, List<ASlice>> rows;
    private final IPartitioner partitioner = StorageService.getPartitioner();
    private final ColumnFamilyStore cfs;
    private final ColumnKey.Comparator comp;

    public Memtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        comp = ColumnKey.getComparator(cfs.table_, cfs.columnFamily_);
        rows = new ConcurrentSkipListMap<ColumnKey, List<ASlice>>(comp);
        creationTime = System.currentTimeMillis();
    }

    /**
     * Flattens lists of slices to an iterator over slices.
     */
    public Iterator<ASlice> concat(Iterator<List<ASlice>> iter)
    {
        Function<List<ASlice>, Iterator<ASlice>> fun = new Function<List<ASlice>, Iterator<ASlice>>()
            {
                public Iterator<ASlice> apply(List<ASlice> list)
                {
                    return list.iterator();
                }
            };
        return Iterators.concat(Iterators.transform(rows.values().iterator(), fun));
    }

    /**
     * Compares two Memtable based on creation time.
     * @param rhs Memtable to compare to.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    public int compareTo(Memtable rhs)
    {
    	long diff = creationTime - rhs.creationTime;
    	if ( diff > 0 )
    		return 1;
    	else if ( diff < 0 )
    		return -1;
    	else
    		return 0;
    }

    public int getCurrentThroughput()
    {
        return currentThroughput.get();
    }
    
    public int getCurrentOperations()
    {
        return currentOperations.get();
    }

    boolean isThresholdViolated()
    {
        return currentThroughput.get() >= this.THRESHOLD || currentOperations.get() >= this.THRESHOLD_COUNT;
    }

    boolean isFrozen()
    {
        return isFrozen;
    }

    void freeze()
    {
        isFrozen = true;
    }

    /**
     * Should only be called by ColumnFamilyStore.apply.  NOT a public API.
     * (CFS handles locking to avoid submitting an op
     *  to a flushing memtable.  Any other way is unsafe.)
    */
    void put(DecoratedKey key, ColumnFamily columnFamily)
    {
        assert !isFrozen; // not 100% foolproof but hell, it's an assert
        resolve(key, columnFamily);
    }

    private void resolve(DecoratedKey key, ColumnFamily cf)
    {
        currentThroughput.addAndGet(cf.size());
        currentOperations.addAndGet(cf.getColumnCount());


        // NB: since we don't support differing metadata for parents yet (aka, range deletes), each Slice here represents a single parent
        List<ASlice> newslices = cf.toSlices(key);
        
        // atomically resolve each parent
        for (ASlice newslice : newslices)
            resolve(Collections.singletonList(newslice));
    }

    /**
     * Atomically resolves the given list of Slices (which must have the same parent) into this Memtable.
     */
    private void resolve(List<ASlice> newslices)
    {
        assert !newslices.isEmpty();

        // determine the key for the parent of these slices
        ColumnKey parentkey = newslices.iterator().next().begin;

        // resolve the new slice against old slices
        List<ASlice> oldslices = rows.get(parentkey);
        int retries = 0;
        while(true)
        {
            if (oldslices == null)
            {
                oldslices = rows.putIfAbsent(parentkey, newslices);
                if (oldslices == null)
                    // successfully added row
                    break;
            }

            // merge
            List<ASlice> mergedslices = new ArrayList<ASlice>();
            Iterators.addAll(mergedslices, new SliceMergingIterator(Arrays.<Iterator>asList(oldslices.iterator(), newslices.iterator()), comp));
            // atomically replace
            // TODO: depends on List.equals(), which is slow: wrap with object that uses reference equality
            if (rows.replace(parentkey, oldslices, mergedslices))
                break;
            else
                oldslices = rows.get(parentkey);

            // TODO: for debugging purposes
            if (retries++ > 1000)
                throw new RuntimeException("Very high contention for " + parentkey);
        }
    }

    // for debugging
    public String contents()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Map.Entry<ColumnKey, List<ASlice>> entry : rows.entrySet())
        {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        builder.append("}");
        return builder.toString();
    }


    private SSTableReader writeSortedContents() throws IOException
    {
        logger.info("Writing " + this);
        SSTableWriter writer = new SSTableWriter(cfs.getFlushPath(), rows.size(), StorageService.getPartitioner());

        DataOutputBuffer buffer = new DataOutputBuffer();

        // combine sorted Slices into sorted ColumnFamilies
        Iterator<Pair<DecoratedKey, ColumnFamily>> rowiter = new SliceToCFIterator(cfs.table_,
                                                                                   cfs.columnFamily_,
                                                                                   concat(rows.values().iterator()));
        while (rowiter.hasNext())
        {
            Pair<DecoratedKey, ColumnFamily> row = rowiter.next();
            buffer.reset();
            /* serialize the cf with column indexes */
            ColumnFamily.serializer().serializeWithIndexes(row.right, buffer);
            /* Now write the key and value to disk */
            writer.append(row.left, buffer);
        }

        SSTableReader ssTable = writer.closeAndOpenReader();
        logger.info("Completed flushing " + ssTable.getFilename());
        return ssTable;
    }

    public void flushAndSignal(final Condition condition, ExecutorService sorter, final ExecutorService writer)
    {
        cfs.getMemtablesPendingFlush().add(this); // it's ok for the MT to briefly be both active and pendingFlush
        writer.submit(new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                cfs.addSSTable(writeSortedContents());
                cfs.getMemtablesPendingFlush().remove(Memtable.this);
                condition.signalAll();
            }
        });
    }

    public String toString()
    {
        return "Memtable(" + cfs.getColumnFamilyName() + ")@" + hashCode();
    }

    /**
     * @param startWith Include data in the result from and including this key and to the end of the memtable
     * @return An iterator of entries with the data from the start key 
     */
    public Iterator<Pair<DecoratedKey, ColumnFamily>> getEntryIterator(DecoratedKey startWith)
    {
        ColumnKey ck = new ColumnKey(startWith, comp.columnDepth());
        return new SliceToCFIterator(cfs.table_, cfs.columnFamily_, concat(rows.tailMap(ck).values().iterator()));
    }

    public boolean isClean()
    {
        return rows.isEmpty();
    }

    public String getTableName()
    {
        return cfs.getTable().name;
    }

    /**
     * obtain an iterator of columns in this memtable in the specified order starting from a given column.
     */
    public static IColumnIterator getSliceIterator(final DecoratedKey key, final ColumnFamily cf, SliceQueryFilter filter, AbstractType typeComparator)
    {
        assert cf != null;
        Collection<IColumn> rawColumns = cf.getSortedColumns();
        Collection<IColumn> filteredColumns = filter.applyPredicate(rawColumns);

        final IColumn columns[] = filteredColumns.toArray(new IColumn[0]);
        // TODO if we are dealing with supercolumns, we need to clone them while we have the read lock since they can be modified later
        if (filter.reversed)
            ArrayUtils.reverse(columns);
        IColumn startIColumn;
        final boolean isStandard = !cf.isSuper();
        if (isStandard)
            startIColumn = new Column(filter.start);
        else
            startIColumn = new SuperColumn(filter.start, null); // ok to not have subcolumnComparator since we won't be adding columns to this object

        // can't use a ColumnComparatorFactory comparator since those compare on both name and time (and thus will fail to match
        // our dummy column, since the time there is arbitrary).
        Comparator<IColumn> comparator = filter.getColumnComparator(typeComparator);
        int index;
        if (filter.start.length == 0 && filter.reversed)
        {
            /* scan from the largest column in descending order */
            index = 0;
        }
        else
        {
            index = Arrays.binarySearch(columns, startIColumn, comparator);
        }
        final int startIndex = index < 0 ? -(index + 1) : index;

        return new AbstractColumnIterator()
        {
            private int curIndex_ = startIndex;

            public ColumnFamily getColumnFamily()
            {
                return cf;
            }

            public DecoratedKey getKey()
            {
                return key;
            }

            public boolean hasNext()
            {
                return curIndex_ < columns.length;
            }

            public IColumn next()
            {
                // clone supercolumns so caller can freely removeDeleted or otherwise mutate it
                return isStandard ? columns[curIndex_++] : ((SuperColumn)columns[curIndex_++]).cloneMe();
            }
        };
    }

    public static IColumnIterator getNamesIterator(final DecoratedKey key, final ColumnFamily cf, final NamesQueryFilter filter)
    {
        assert cf != null;
        final boolean isStandard = !cf.isSuper();

        return new SimpleAbstractColumnIterator()
        {
            private Iterator<byte[]> iter = filter.columns.iterator();
            private byte[] current;

            public ColumnFamily getColumnFamily()
            {
                return cf;
            }

            public DecoratedKey getKey()
            {
                return key;
            }

            protected IColumn computeNext()
            {
                while (iter.hasNext())
                {
                    current = iter.next();
                    IColumn column = cf.getColumn(current);
                    if (column != null)
                        // clone supercolumns so caller can freely removeDeleted or otherwise mutate it
                        return isStandard ? column : ((SuperColumn)column).cloneMe();
                }
                return endOfData();
            }
        };
    }

    /**
     * FIXME: Squashing Slices back into a CF is inefficient.
     */
    public ColumnFamily getColumnFamily(DecoratedKey dk)
    {
        ColumnKey ck = new ColumnKey(dk, comp.columnDepth());
        Iterator<Pair<DecoratedKey, ColumnFamily>> rowiter = new SliceToCFIterator(cfs.table_,
                                                                                   cfs.columnFamily_,
                                                                                   concat(rows.tailMap(ck).values().iterator()));
        if (!rowiter.hasNext())
            return null;

        // collect a single row
        Pair<DecoratedKey, ColumnFamily> row = rowiter.next();
        // and confirm that it has a matching key
        if (!dk.equals(row.left))
            return null;
        return row.right;
    }

    void clearUnsafe()
    {
        rows.clear();
    }

    public boolean isExpired()
    {
        return System.currentTimeMillis() > creationTime + DatabaseDescriptor.getMemtableLifetimeMS();
    }
}
