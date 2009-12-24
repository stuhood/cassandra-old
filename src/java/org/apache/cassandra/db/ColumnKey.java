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

package org.apache.cassandra.db;

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
    // singleton empty name
    private final byte[] EMPTY_NAME = new byte[0];

    public final DecoratedKey dk;
    // FIXME: more efficient structure? perhaps model after Slice.Metadata?
    public final byte[][] names;

    /**
     * Creates a column key wrapping a DecoratedKey, but with empty names to the
     * given depth.
     */
    public ColumnKey(DecoratedKey dk, int depth)
    {
        assert 0 < depth && depth < Byte.MAX_VALUE;
        this.dk = dk;
        this.names = new byte[depth][];
        for (int i = 0; i < names.length; i++)
            this.names[i] = EMPTY_NAME;
    }

    public ColumnKey(DecoratedKey dk, byte[]... names)
    {
        assert names.length < Byte.MAX_VALUE;
        this.dk = dk;
        this.names = names;
    }

    /**
     * @return A clone of this ColumnKey, with the given value as the least
     * significant name.
     */
    public ColumnKey withName(byte[] name)
    {
        assert names.length > 0 && name != null;

        // shallow copy of the names
        byte[][] namesClone = Arrays.copyOf(names, names.length);
        namesClone[namesClone.length-1] = name;
        return new ColumnKey(dk, namesClone);
    }

    /**
     * @return A clone of this ColumnKey, with the least significant name cleared.
     */
    public ColumnKey parent()
    {
        return withName(EMPTY_NAME);
    }

    /**
     * @return The name at the given depth: the dk is at depth 0, so names
     * begin at depth 1.
     */
    public byte[] name(int depth)
    {
        return names[depth-1];
    }

    /**
     * Returns a comparator that compares ColumnKeys by dk and then names, using
     * the appropriate comparator at each level. The ColumnKeys must contain
     * an equal number of names: for example, a ColumnFamily containing columns
     * of type super, should always have name.length == 2, although tailing empty
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
        if (that.dk.compareTo(this.dk) != 0)
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
        components[0] = dk.hashCode();
        for (byte i = 0; i < names.length; i++)
            components[i+1] = Arrays.hashCode(names[i]);
        return Arrays.hashCode(components);
    }

    public void serialize(DataOutput dos) throws IOException
    {
        dos.writeUTF(StorageService.getPartitioner().convertToDiskFormat(dk));

        dos.writeByte((byte)names.length);
        for (byte[] name : names)
            ColumnSerializer.writeName(name, dos);
    }

    public static ColumnKey deserialize(DataInput dis) throws IOException
    {
        DecoratedKey dk =
            StorageService.getPartitioner().convertFromDiskFormat(dis.readUTF());

        byte nameCount = dis.readByte();
        byte[][] names = new byte[nameCount][];
        for (int i = 0; i < nameCount; i++)
            names[i] = ColumnSerializer.readName(dis);
        return new ColumnKey(dk, names);
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
         * just the dk field, depth 1 will compare the dk and the first name, etc.
         * This can be used to find the boundries of slices of columns.
         *
         * A null name compares as less than a non-null name, meaning that you
         * can match the beginning of a slice with a ColumnKey with tailing null
         * names.
         */
        public int compare(ColumnKey o1, ColumnKey o2, int depth)
        {
            // reference equality of CK
            if (o1 == o2) return 0;

            // sanity checks
            assert depth < Byte.MAX_VALUE;
            assert o1.names.length == o2.names.length;

            // reference equality of DK
            if (o1.dk != o2.dk)
            {
                int comp = o1.dk.compareTo(o2.dk);
                if (comp != 0) return comp;
            }
            for (int i = 0; i < depth; i++)
            {
                // reference equality of Name
                if (o1.names[i] == o2.names[i]) continue;
                
                // nulls compare less than non-nulls
                if (o1.names[i] == null) return -1;
                if (o2.names[i] == null) return 1;

                // type based comparison
                int comp = nameComparators[i].compare(o1.names[i], o2.names[i]);
                if (comp != 0) return comp;
            }
            return 0;
        }

        /**
         * FIXME: We can definitely find a better way to store the CK in a bloom,
         * and we should before release.
         */
        public String forBloom(ColumnKey key)
        {
            StringBuilder buff = new StringBuilder();
            buff.append(StorageService.getPartitioner().convertToDiskFormat(key.dk));
            for (int i = 0; i < key.names.length; i++)
                buff.append("\u0000").append(nameComparators[i].getString(key.names[i]));
            return buff.toString();
        }
    }
}
