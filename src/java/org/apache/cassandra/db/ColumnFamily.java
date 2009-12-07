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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;


public final class ColumnFamily extends AColumnFamily implements IColumnContainer
{
    private static Logger logger_ = Logger.getLogger( ColumnFamily.class );

    public static ColumnFamily create(String tableName, String cfName)
    {
        String columnType = DatabaseDescriptor.getColumnFamilyType(tableName, cfName);
        AbstractType comparator = DatabaseDescriptor.getComparator(tableName, cfName);
        AbstractType subcolumnComparator = DatabaseDescriptor.getSubComparator(tableName, cfName);
        return new ColumnFamily(cfName, columnType, comparator, subcolumnComparator);
    }

    long markedForDeleteAt = Long.MIN_VALUE;
    int localDeletionTime = Integer.MIN_VALUE;
    private final AtomicInteger size_ = new AtomicInteger(0);
    protected ConcurrentSkipListMap<byte[], IColumn> columns_;

    public ColumnFamily(String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator)
    {
        super(cfName, columnType, subcolumnComparator);
        columns_ = new ConcurrentSkipListMap<byte[], IColumn>(comparator);
    }

    public ColumnFamily(String cfName, String columnType, ConcurrentSkipListMap<byte[], IColumn> columns, AbstractType subcolumnComparator)
    {
        super(cfName, columnType, subcolumnComparator);
        columns_ = columns;
    }

    public ColumnFamily asMutable()
    {
        return this;
    }

    public Collection<IColumn> getColumns()
    {
        return columns_.values();
    }

    public IColumn getColumn(byte[] name)
    {
        return columns_.get(name);
    }

    /*
     *  We need to go through each column
     *  in the column family and resolve it before adding
    */
    public void addAll(AColumnFamily cf)
    {
        for (IColumn column : cf.getColumns())
        {
            addColumn(column);
        }
        delete(cf);
    }

    public void addColumn(QueryPath path, byte[] value, long timestamp)
    {
        addColumn(path, value, timestamp, false);
    }

    /** In most places the CF must be part of a QueryPath but here it is ignored. */
    public void addColumn(QueryPath path, byte[] value, long timestamp, boolean deleted)
	{
        assert path.columnName != null : path;
		IColumn column;
        if (path.superColumnName == null)
        {
            column = new Column(path.columnName, value, timestamp, deleted);
        }
        else
        {
            assert isSuper();
            column = new SuperColumn(path.superColumnName, getSubComparator());
            column.addColumn(new Column(path.columnName, value, timestamp, deleted)); // checks subcolumn name
        }
		addColumn(column);
    }

    public void clear()
    {
    	columns_.clear();
    	size_.set(0);
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column .
    */
    public void addColumn(IColumn column)
    {
        byte[] name = column.name();
        IColumn oldColumn = columns_.get(name);
        if (oldColumn != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                int oldSize = oldColumn.size();
                ((SuperColumn) oldColumn).putColumn(column);
                size_.addAndGet(oldColumn.size() - oldSize);
            }
            else
            {
                if (((Column)oldColumn).comparePriority((Column)column) <= 0)
                {
                    columns_.put(name, column);
                    size_.addAndGet(column.size());
                }
            }
        }
        else
        {
            size_.addAndGet(column.size());
            columns_.put(name, column);
        }
    }

    public void remove(byte[] columnName)
    {
    	columns_.remove(columnName);
    }

    public void delete(int localtime, long timestamp)
    {
        localDeletionTime = localtime;
        markedForDeleteAt = timestamp;
    }

    public void delete(AColumnFamily cf2)
    {
        delete(Math.max(getLocalDeletionTime(), cf2.getLocalDeletionTime()),
               Math.max(getMarkedForDeleteAt(), cf2.getMarkedForDeleteAt()));
    }

    protected ConcurrentSkipListMap<byte[],IColumn> cloneColumns()
    {
        return columns_.clone();
    }

    public AbstractType getComparator()
    {
        return (AbstractType)columns_.comparator();
    }

    int size()
    {
        if (size_.get() == 0)
        {
            for (IColumn column : columns_.values())
            {
                size_.addAndGet(column.size());
            }
        }
        return size_.get();
    }

    public long getMarkedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    public void setMarkedForDeleteAt(long markedForDeleteAt)
    {
        this.markedForDeleteAt = markedForDeleteAt;
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime;
    }

    public void setLocalDeletionTime(int localDeletionTime)
    {
        this.localDeletionTime = localDeletionTime;
    }

    public void resolve(AColumnFamily cf)
    {
        // Row _does_ allow null CF objects :(  seems a necessary evil for efficiency
        if (cf == null)
            return;
        delete(cf);
        addAll(cf);
    }
}
