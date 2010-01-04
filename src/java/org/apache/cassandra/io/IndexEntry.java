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

package org.apache.cassandra.io;

import java.io.*;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageService;


/**
 * An entry in the SSTable index file which points to a block in the data file.
 * Each entry contains the full path of the column at the beginning of the block
 * in the SSTable, and two file positions: the offset of the serialized version
 * of this object in the index, and the offset of the block in the data file.
 *
 * To find a key in the data file, we first look at IndexEntries in memory, and find
 * the last entry less than the key we want. We then seek to the position of that
 * entry in the index file, read forward until we find the last entry less than the
 * key, and then seek to the position of that entry's block in the data file.
 */
public class IndexEntry extends ColumnKey
{
    public final long indexOffset;
    public final long dataOffset;
    public final int blockLen;

    public IndexEntry(DecoratedKey key, byte[][] names, long indexOffset, long dataOffset, int blockLen)
    {
        super(key, names);
        this.indexOffset = indexOffset;
        this.dataOffset = dataOffset;
        this.blockLen = blockLen;
    }

    /**
     * Serializes this IndexEntry into the index file.
     */
    @Override
    public void serialize(DataOutput dos) throws IOException
    {
        super.serialize(dos);
        // note: only the dataOffset is serialized to disk, because in order
        // to deserialize this value, we will need to know indexOffset anyway
        dos.writeLong(dataOffset);
        dos.writeInt(blockLen);
    }

    /**
     * Deserializes an IndexEntry from the index file.
     */
    public static IndexEntry deserialize(RandomAccessFile dis) throws IOException
    {
        long indexOffset = dis.getFilePointer();
        ColumnKey key = ColumnKey.deserialize(dis);
        long dataOffset = dis.readLong();
        int blockLen = dis.readInt();
        return new IndexEntry(key.dk, key.names, indexOffset, dataOffset, blockLen);
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<IndexEntry ").append(super.toString()).append(" ioffset=")
            .append(indexOffset).append(" doffset=").append(dataOffset).append(">");
        return buff.toString();
    }
}
