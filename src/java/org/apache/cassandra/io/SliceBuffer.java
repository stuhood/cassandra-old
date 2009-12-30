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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.security.MessageDigest;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;

/**
 * Extends Slice to add serialized or realized columns. At least 1 of the 2 will be set
 * at a given time: if the realized columns are modified, the buffer is cleared, and
 * lazily recreated. Likewise, if the buffer is modified, the realized columns will be
 * lazily deserialized.
 */
public class SliceBuffer extends Slice
{
    private DataOutputBuffer serialized;
    private List<Column> realized;

    SliceBuffer(Slice.Metadata meta, ColumnKey key, ColumnKey nextKey, List<Column> realized)
    {
        super(meta, key, nextKey, realized.size());
        assert this.realized != null;
        this.realized = realized;
    }

    SliceBuffer(Slice.Metadata meta, ColumnKey key, ColumnKey nextKey, int numCols, DataOutputBuffer serialized)
    {
        super(meta, key, nextKey, numCols);
        assert this.serialized != null;
        this.serialized = serialized;
    }

    public DataOutputBuffer serialized()
    {
        if (serialized != null)
            return serialized;
        
        // serialize the columns
        serialized = new DataOutputBuffer(numCols * 10);
        for (Column col : realized)
            Column.serializer().serialize(col, serialized);
        return serialized;
    }

    public List<Column> realized()
    {
        if (realized != null)
            return realized;

        // realize the columns from the buffer
        Column[] cols = new Column[numCols];
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(serialized.getData(),
                                                                              0, serialized.getLength()));
        try
        {
            for (int i = 0; i < cols.length; i++)
                cols[i] = (Column)Column.serializer().deserialize(stream);
            realized = Arrays.asList(cols);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        return realized;
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
        if (numCols > 0)
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

        if (numCols == 0)
            // for tombstones, only metadata is digested
            digest.update(shared.getData(), 0, shared.getLength());
        for (Column col : realized())
        {
            digest.update(shared.getData(), 0, shared.getLength());
            col.updateDigest(digest);
        }
    }
}
