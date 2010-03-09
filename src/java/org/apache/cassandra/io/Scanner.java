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
     * @return The approximate number of bytes remaining in the Scanner.
     */
    public long getBytesRemaining();

    public boolean first() throws IOException;
    public boolean seekNear(DecoratedKey seekKey) throws IOException;
    public boolean seekNear(ColumnKey seekKey) throws IOException;

    /**
     * Releases any resources associated with this scanner.
     */
    public void close() throws IOException;
}
