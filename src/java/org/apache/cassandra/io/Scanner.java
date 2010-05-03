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

package org.apache.cassandra.io;

import java.io.*;
import java.util.Iterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.filter.INameFilter;

import com.google.common.collect.PeekingIterator;

/**
 * A Scanner is an Iterator for reading slices in a certain order.
 *
 * After creation, the Scanner is not positioned at a slice. Call first() or
 * seek*() to position it immediately before a Slice, and then use next() to iterate.
 */
public interface Scanner extends Iterator<SliceBuffer>,Closeable
{
    /**
     * @return The depth of the Columns in this SSTable.
     */
    public int columnDepth();

    /**
     * @return The Comparator for the returned Slices.
     */
    public ColumnKey.Comparator comparator();

    /**
     * Sets a INameFilter to use to minimize deserialization by filtering columns as close to the source as possible.
     * Slices which have all of their columns filtered out will still be returned in case their metadata needs to be
     * applied elsewhere. To filter large ranges, it's more efficient to use a QueryFilter with FilteredScanner,
     * which can seek on the underlying scanner.
     *
     * @param filter A filter to be applied to the columns in returned Slices.
     * @return A QueryFilter which describes the portion of the filtering that cannot be pushed down by this scanner.
     */
    public void pushdownFilter(INameFilter filter);

    /**
     * @return The approximate number of bytes remaining in the Scanner.
     */
    public long getBytesRemaining();

    public boolean first();
    public boolean seekNear(DecoratedKey seekKey) throws IOException;
    public boolean seekNear(ColumnKey seekKey) throws IOException;

    /**
     * Releases any resources associated with this scanner.
     */
    public void close() throws IOException;
}
