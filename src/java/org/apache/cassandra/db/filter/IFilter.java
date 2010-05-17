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
 * Given an implementation-specific description of what columns to look for, provides methods
 * to extract the desired columns from a Memtable, SSTable, or SuperColumn.  Either the get*ColumnIterator
 * methods will be called, or filterSuperColumn, but not both on the same object.  QueryFilter
 * takes care of putting the two together if subcolumn filtering needs to be done, based on the
 * querypath that it knows (but that IFilter implementations are oblivious to).
 */
public interface IFilter<T>
{
    /**
     * @return True if this filter might match columns between the given names.
     */
    public abstract boolean matchesBetween(T begin, T end);

    /**
     * @return True if this filter matches the given name (assuming that parents match).
     */
    public abstract boolean matches(T name);
}
