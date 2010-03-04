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
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;

/**
 * A Scanner is an abstraction for reading Slices in a certain order.
 */
public interface Scanner extends Closeable
{
    public long getBytesRemaining();

    public boolean first() throws IOException;
    public boolean seekNear(DecoratedKey seekKey) throws IOException;
    public boolean seekNear(ColumnKey seekKey) throws IOException;
    public boolean next() throws IOException;

    public Slice get();
    public List<Column> getColumns() throws IOException;
    public SliceBuffer getBuffer() throws IOException;
}
