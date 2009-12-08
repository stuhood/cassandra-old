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

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ICompactSerializer2;

import com.google.common.collect.ImmutableSortedMap;

public final class ImmutableColumnFamily extends AColumnFamily
{
    private static Logger logger_ = Logger.getLogger( ImmutableColumnFamily.class );

    public static final class Builder
    {
        public final String cfName;
        public final String columnType;
        public final AbstractType comparator;
        public final AbstractType subcolumnComparator;
        public final ICompactSerializer2<IColumn> columnSerializer;

        private ImmutableSortedMap.Builder mapbuilder;
        private long markedForDeleteAt;
        private int localDeletionTime;

        public Builder(String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator)
        {
            this.cfName = cfName;
            this.columnType = columnType;
            this.comparator = comparator;
            this.subcolumnComparator = subcolumnComparator;
            this.columnSerializer = columnType.equals("Standard") ?
                Column.serializer() :
                SuperColumn.serializer(subcolumnComparator);
            clear();
        }

        public void setMarkedForDeleteAt(long markedForDeleteAt)
        {
            this.markedForDeleteAt = markedForDeleteAt;
        }

        public void setLocalDeletionTime(int localDeletionTime)
        {
            this.localDeletionTime = localDeletionTime;
        }

        public void add(IColumn column)
        {
            mapbuilder.put(column.name(), column);
        }

        /**
         * Clear this Builder for reuse: putting it back to the state it was in
         * after construction.
         */
        public void clear()
        {
            mapbuilder = new ImmutableSortedMap.Builder(comparator);
            markedForDeleteAt = Long.MIN_VALUE;
            localDeletionTime = Integer.MIN_VALUE;
        }

        public ImmutableColumnFamily build()
        {
            return new ImmutableColumnFamily(cfName, columnType, mapbuilder.build(), markedForDeleteAt,
                                             localDeletionTime, columnSerializer);
        }
    }

    /**
     * Return a builder that can be (re)used to create a column family for the given
     * table.
     */
    public static ImmutableColumnFamily.Builder builder(String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator)
    {
        return new ImmutableColumnFamily.Builder(cfName, columnType, comparator, subcolumnComparator);
    }

    private final long markedForDeleteAt;
    private final int localDeletionTime;
    private final int size;
    private final ImmutableSortedMap<byte[],IColumn> columns;

    /**
     * Constructs an empty column family. To build an immutable column family
     * containing columns, see ImmutableColumnFamily.builder().
     */
    private ImmutableColumnFamily(String cfName, String columnType, ImmutableSortedMap<byte[],IColumn> columns, long markedForDeleteAt, int localDeletionTime, ICompactSerializer2<IColumn> columnSerializer)
    {
        super(cfName, columnType, columnSerializer);
        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTime = localDeletionTime;
        this.columns = columns;

        int colsize = 0;
        for (IColumn column : columns.values())
            colsize += column.size();
        this.size = colsize;
    }

    public ColumnFamily asMutable()
    {
        ColumnFamily mutable = new ColumnFamily(name, type, getColumnSerializer(), cloneColumns());
        mutable.setMarkedForDeleteAt(markedForDeleteAt);
        mutable.setLocalDeletionTime(localDeletionTime);
        return mutable;
    }

    public Collection<IColumn> getColumns()
    {
        return columns.values();
    }

    public IColumn getColumn(byte[] name)
    {
        return columns.get(name);
    }

    protected ConcurrentSkipListMap<byte[],IColumn> cloneColumns()
    {
        return new ConcurrentSkipListMap<byte[],IColumn>(columns);
    }

    public AbstractType getComparator()
    {
        return (AbstractType)columns.comparator();
    }

    int size()
    {
        return size;
    }

    public long getMarkedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime;
    }
}
