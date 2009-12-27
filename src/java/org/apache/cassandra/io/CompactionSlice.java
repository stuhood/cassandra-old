package org.apache.cassandra.io;
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

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;

/**
 * Extends Slice to add a list of Columns.
 */
public class CompactionSlice extends Slice
{
    public final List<Column> columns;

    public CompactionSlice(ColumnKey key, Slice.Metadata meta)
    {
        super(meta, key);
        this.columns = new ArrayList<Column>();
    }

    /**
     * Digest the parent portion of the key, the metadata and the content of each
     * column sequentially.
     *
     * NB: A sequence of columns with the same parents and metadata should always
     * result in the same digest, no matter how it is split.
     */
    public byte[] digest()
    {
        // MerkleTree uses XOR internally, so we want lots of output bits here
        // FIXME: byte[] rowhash = FBUtilities.hash("SHA-256", slice.key.key.getBytes(), row.buffer.getData());
        throw new RuntimeException("Not implemented."); // FIXME
    }
}
