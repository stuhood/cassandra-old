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

import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * FIXME: A stub to allow compilation of IFilter implementations that are only used to implement matching.
 */
@Deprecated
public abstract class StubFilter<T> implements IFilter<T>
{
    public IColumnIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key, AbstractType comparator)
    {
        throw new RuntimeException("Not implemented");
    }

    public IColumnIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, long dataStart)
    {
        throw new RuntimeException("Not implemented");
    }

    public IColumnIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key)
    {
        throw new RuntimeException("Not implemented");
    }

    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore)
    {
        throw new RuntimeException("Not implemented");
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        throw new RuntimeException("Not implemented");
    }

    public Comparator<IColumn> getColumnComparator(AbstractType comparator)
    {
        throw new RuntimeException("Not implemented");
    }
}
