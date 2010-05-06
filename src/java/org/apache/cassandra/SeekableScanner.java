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

package org.apache.cassandra;

import java.io.*;
import java.util.Iterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.filter.IFilter;

/**
 * A SeekableScanner is an Iterator for reading slices in sorted order, which is
 * capable of resetting or seeking to other positions.
 *
 * After creation, a SeekableScanner is positioned immediately before the first slice.
 * Call seek*() or first() to reposition it.
 */
public interface SeekableScanner extends Scanner
{
    /**
     * Sets an IFilter to use to minimize deserialization by filtering columns as close to the source as possible.
     * Slices which have all of their columns filtered out will still be returned in case their metadata needs to be
     * applied elsewhere.
     *
     * @param filter A filter to be applied to the columns in returned Slices.
     */
    public void pushdownFilter(IFilter<byte[]> filter);

    /**
     * Seek to the first Slice in the Scanner.
     * @return False if the Scanner contains no Slices.
     */
    public boolean first();

    /**
     * See the contract for seekNear(CK).
     */
    public boolean seekNear(DecoratedKey seekKey) throws IOException;

    /**
     * Seeks to the slice which might contain the given key, without checking the existence of the key. If such a slice
     * does not exist, the next calls to next() will have undefined results.
     *
     * seekKeys with trailing NAME_BEGIN or NAME_END names will properly match the slices that they begin or end when
     * used with this method.
     *
     * @return False if no such Slice was found.
     */
    public boolean seekNear(ColumnKey seekKey) throws IOException;

    /**
     * See the contract for seekTo(CK).
     */
    public boolean seekTo(DecoratedKey seekKey) throws IOException;

    /**
     * Seeks to the slice which might contain the given key. If the key does not
     * exist, or such a slice does not exist, the next calls to next() will have
     * undefined results.
     *
     * @return False if no such Slice was found.
     */
    public boolean seekTo(ColumnKey seekKey) throws IOException;
}
