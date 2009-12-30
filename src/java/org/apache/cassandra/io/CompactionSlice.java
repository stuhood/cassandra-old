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

import java.io.IOException;
import java.util.*;
import java.security.MessageDigest;

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
     * Calculates whether the Metadata representing this Slice as a tombstone should
     * be removed.
     *
     * @return True if the Slice is empty, and it was marked deleted long enough ago.
     */
    public boolean isDeleted(boolean major, int gcBefore)
    {
        if (!major)
            // tombstones cannot be removed without a major compaction
            return false;
        if (!columns.isEmpty())
            // this Slice is not a tombstone, so it can't be removed
            return false;
        if (meta.getLocalDeletionTime() > gcBefore)
            // a component of our metadata is too young to be gc'd
            return false;
        return true;
    }

    /**
     * For each column, adds the parent portion of the key, the metadata and the
     * content of the column to the given digest.
     *
     * An empty slice (acting only as a tombstone), will digest only the key and
     * metadata.
     */
    public void updateDigest(MessageDigest digest)
    {
        // parent data that is shared for these columns
        DataOutputBuffer shared = new DataOutputBuffer();
        try
        {
            meta.serialize(shared);
            key.withName(ColumnKey.NAME_BEGIN).serialize(shared);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        if (columns.isEmpty())
            digest.update(shared.getData(), 0, shared.getLength());
        for (Column col : columns)
        {
            digest.update(shared.getData(), 0, shared.getLength());
            col.updateDigest(digest);
        }
    }
}
