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
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageService;


/**
 * An entry in the SSTable index file. Each entry contains the full path to a column
 * in the SSTable, and the offset of the column in the SSTable data file.
 */
public class IndexEntry extends ColumnKey
{
    public final long offset;

    public IndexEntry(DecoratedKey key, byte[][] names, long offset)
    {
        super(key, names);
        this.offset = offset;
    }

    public void serialize(DataOutput dos) throws IOException
    {
        dos.writeUTF(StorageService.getPartitioner().convertToDiskFormat(key));

        dos.writeByte((byte)names.length);
        for (byte[] name : names)
            ColumnSerializer.writeName(name, dos);
        dos.writeLong(offset);
    }

    public static IndexEntry deserialize(RandomAccessFile dis) throws IOException
    {
        final DecoratedKey key =
            StorageService.getPartitioner().convertFromDiskFormat(dis.readUTF());

        final byte nameCount = dis.readByte();
        final byte[][] names = new byte[nameCount][];
        for (int i = 0; i < nameCount; i++)
            names[i] = ColumnSerializer.readName(dis);
        final long offset = dis.readLong();
        return new IndexEntry(key, names, offset);
    }
}
