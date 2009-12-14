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
 * The full path to a column in a column family.
 */
public class ColumnKey
{
    public final DecoratedKey key;
    // FIXME: safer structure?
    public final byte[][] names;

    public ColumnKey(DecoratedKey key, byte[][] names)
    {
        assert names.length < Byte.MAX_VALUE;
        this.key = key;
        this.names = names;
    }

    /**
     * Returns a comparator that compares ColumnKeys by key and then names, using
     * the appropriate comparator at each level. The ColumnKeys must contain
     * an equal number of names: for example, a ColumnFamily containing columns
     * of type super, should always have name.length == 2, although tailing null
     * names can be used to match the beginning of a subrange for instance.
     */
    public static ColumnKey.Comparator getComparator(String table, String cf)
    {
        if("Super".equals(DatabaseDescriptor.getColumnFamilyType(table, cf)))
            return new Comparator(DatabaseDescriptor.getComparator(table, cf),
                                            DatabaseDescriptor.getSubComparator(table, cf));
        return new Comparator(DatabaseDescriptor.getComparator(table, cf));
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ColumnKey))
            return false;
        ColumnKey that = (ColumnKey)o;
        if (that.key.compareTo(this.key) != 0)
            return false;
        if (this.names.length != that.names.length)
            return false;
        for (byte i = 0; i < this.names.length; i++)
            if (!Arrays.equals(this.names[i], that.names[i]))
                return false;
        return true;
    }

    @Override
    public int hashCode()
    {
        int[] components = new int[1+names.length];
        components[0] = key.hashCode();
        for (byte i = 0; i < names.length; i++)
            components[i+1] = Arrays.hashCode(names[i]);
        return Arrays.hashCode(components);
    }

    public void serialize(DataOutput dos) throws IOException
    {
        dos.writeUTF(StorageService.getPartitioner().convertToDiskFormat(key));

        dos.writeByte((byte)names.length);
        for (byte[] name : names)
            ColumnSerializer.writeName(name, dos);
    }

    public static ColumnKey deserialize(DataInput dis) throws IOException
    {
        DecoratedKey key =
            StorageService.getPartitioner().convertFromDiskFormat(dis.readUTF());

        byte nameCount = dis.readByte();
        byte[][] names = new byte[nameCount][];
        for (int i = 0; i < nameCount; i++)
            names[i] = ColumnSerializer.readName(dis);
        return new ColumnKey(key, names);
    }

    /**
     * A Comparator that supports comparing ColumnKeys at an arbitrary depth.
     * The implementation of java.util.Comparator.compare() uses the maximum
     * depth.
     */
    public static class Comparator implements java.util.Comparator<ColumnKey>
    {
        final AbstractType[] nameComparators;
        public Comparator(AbstractType... nameComparators)
        {
            this.nameComparators = nameComparators;
        }

        public int compare(ColumnKey o1, ColumnKey o2)
        {
            return compare(o1, o2, o1.names.length);
        }

        /**
         * Compares the given column keys to the given depth. Depth 0 will compare
         * just the key field, depth 1 will compare the key and the first name, etc.
         * This can be used to find the boundries of slices of columns.
         */
        public int compare(ColumnKey o1, ColumnKey o2, int depth)
        {
            assert depth < Byte.MAX_VALUE;
            assert o1.names.length == o2.names.length;
            int comp = o1.key.compareTo(o2.key);
            if (comp != 0)
                return comp;
            for (int i = 0; i < depth; i++)
            {
                comp = nameComparators[i].compare(o1.names[i], o2.names[i]);
                if (comp != 0)
                    return comp;
            }
            return 0;
        }

        /**
         * FIXME: We can definitely find a better way to store the CK in a bloom filter, and we should before release.
         */
        public String forBloom(ColumnKey key)
        {
            StringBuilder buff = new StringBuilder();
            buff.append(StorageService.getPartitioner().convertToDiskFormat(key.key));
            for (int i = 0; i < key.names.length; i++)
                buff.append("\u0000").append(nameComparators[i].getString(key.names[i]));
            return buff.toString();
        }
    }
}
