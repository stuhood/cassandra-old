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
 * An immutable object representing a Slice read from disk: A Slice is a sorted
 * sequence of columns within a SSTable block that share the same parents, and thus
 * the same Metadata.
 */
public class Slice
{
    public final Metadata parentMeta;
    // immutable list of immutable columns
    public final List<Column> columns;

    /**
     * @param parentMeta Metadata for the parents of this Slice.
     * @param columns Once ownership of the column list is passed to a Slice,
     *        it should not be modified.
     */
    Slice(Metadata parentMeta, List<Column> columns)
    {
        this.parentMeta = parentMeta;
        this.columns = Collections.unmodifiableList(columns);
    }

    /**
     * Serializes this Slice into the index file.
     */
    @Override
    public void serialize(DataOutput dos) throws IOException
    {
        super.serialize(dos);
        // note: only the dataOffset is serialized to disk, because in order
        // to deserialize this value, we will need to know indexOffset anyway
        dos.writeLong(dataOffset);
    }

    /**
     * Deserializes an Slice from the index file.
     */
    public static Slice deserialize(RandomAccessFile dis) throws IOException
    {
        long indexOffset = dis.getFilePointer();
        ColumnKey key = ColumnKey.deserialize(dis);
        long dataOffset = dis.readLong();
        return new Slice(key.key, key.names, indexOffset, dataOffset);
    }

    /**
     * Skips a single Slice in the given file.
     */
    public static void skip(RandomAccessFile dis) throws IOException
    {
        dis.readUTF();
        byte nameCount = dis.readByte();
        while (nameCount-- > 0)
            ColumnSerializer.readName(dis);
        dis.readLong();
    }

    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<Slice ").append(super.toString()).append(" ioffset=")
            .append(indexOffset).append(" doffset=").append(dataOffset).append(">");
        return buff.toString();
    }

    /**
     * Metadata shared between columns in a Slice. Currently contains deletion info.
     */
    static class Metadata
    {
        // ("markedForDeleteAt","localDeletionTime") for parents of the slice
        // FIXME: @see SSTableWriter.append()
        public final List<Pair<Long,Integer>> meta;

        public Metadata()
        {
            this(Collections.<Pair<Long,Integer>>emptyList());
        }

        public Metadata(List<Pair<Long,Integer>> meta)
        {
            assert meta.size() < Byte.MAX_VALUE;
            this.meta = meta;
        }

        public void serialize(DataOutput dos) throws IOException
        {
            dos.writeByte((byte)meta.size());
            for (Pair<Long,Integer> val : meta)
            {
                dos.writeLong(val.left);
                dos.writeInt(val.right);
            }
        }

        public static Metadata deserialize(DataInput dis) throws IOException
        {
            byte metaLen = dis.readByte();
            for (int i = 0; i < metaLen; i++)
                meta.add(new Pair<Long,Integer>(dis.readLong(), dis.readInt()));
            return new Metadata(meta);
        }
    }
}
