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
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;

/**
 * The full path to a column in a column family.
 */
public class ColumnKey
{
    // singleton names representing the beginning and end of a subrange
    public static final byte[] NAME_BEGIN = new byte[0];
    public static final byte[] NAME_END = null;

    public final DecoratedKey dk;
    // FIXME: more efficient structure? perhaps model after Slice.Metadata?
    public final byte[][] names;

    /**
     * Creates a column key of the given depth, and the given names. Any names
     * not specified will be NAME_BEGIN.
     */
    public ColumnKey(DecoratedKey dk, int depth, byte[]... names)
    {
        assert 0 < depth && depth < Byte.MAX_VALUE;
        this.dk = dk;
        if (names == null || names.length != depth)
        {
            this.names = new byte[depth][];
            for (int i = 0; i < names.length; i++)
                this.names[i] = NAME_BEGIN;

            if (names != null)
            {
                assert this.names.length > names.length;
                System.arraycopy(names, 0, this.names, 0, names.length);
            }
        }
        else
        {
            this.names = names;
        }
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
     * @return The name at the given depth: the dk is at depth 0, so names
     * begin at depth 1.
     */
    public byte[] name(int depth)
    {
        return names[depth-1];
    }

    /**
     * Returns a comparator that compares ColumnKeys by dk and then names, using
     * the appropriate type comparator at each level. The ColumnKeys must contain
     * an equal number of names: for example, a ColumnFamily containing columns
     * of type super should always have name.length == 2.
     *
     * Tailing NAME_BEGIN names can be used to match the beginning of a subrange,
     * while tailing NAME_END names will match the end of a subrange.
     */
    public static ColumnKey.Comparator getComparator(String table, String cf)
    {
        if("Super".equals(DatabaseDescriptor.getColumnFamilyType(table, cf)))
            return new Comparator(DatabaseDescriptor.getComparator(table, cf),
                                  DatabaseDescriptor.getSubComparator(table, cf));
        return new Comparator(DatabaseDescriptor.getComparator(table, cf));
    }

    /**
     * @return The components of this key that are stored in a bloom filter.
     */
    List<byte[]> getBloomComponents()
    {
        List<byte[]> components = new ArrayList<byte[]>(1+names.length);
        // key only
        DataOutputBuffer buff = new DataOutputBuffer();
        try
        {
            buff.writeUTF(dk.key);
            components.add(buff.toByteArray());
            for (int i = 0; i < names.length; i++)
            {
                if (names[i] == null || names[i].length < 1)
                    // beginning of trailing null names
                    break;
                buff.write(names[i]);
                // with name at depth i+1 appended
                components.add(buff.toByteArray());
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        return components;
    }

    /**
     * Adds all components of this column key to the bloom filter.
     */
    public void addToBloom(BloomFilter bf)
    {
        for (byte[] component : getBloomComponents())
            bf.add(component);
    }

    /**
     * Checks that all components of this column key are contained in the bloom filter.
     */
    public boolean isPresentInBloom(BloomFilter bf)
    {
        for (byte[] component : getBloomComponents())
            if (!bf.isPresent(component))
                return false;
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
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
        return Arrays.hashCode(new int[]{dk.hashCode(), Arrays.hashCode(names)});
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

        /**
         * Compares the given names as if they were at the given depth. Depths begin at 1.
         *
         * NB: Should replace direct usage of AbstractType, since it adds null checking.
         */
        public int compareAt(byte[] o1, byte[] o2, int depth)
        {
            // reference equality of Name
            if (o1 == o2) return 0;
            
            // nulls compare greater than non-nulls
            if (o1 == null) return 1;
            if (o2 == null) return -1;

            // type based comparison
            return nameComparators[depth-1].compare(o1, o2);
        }

        /**
         * Implement Comparator.compare() by comparing at the maximum depth.
         */
        public int compare(ColumnKey o1, ColumnKey o2)
        {
            return compare(o1, o2, o1.names.length);
        }

        /**
         * Compares the given column keys to the given depth. Depth 0 will compare
         * just the dk field, depth 1 will compare the dk and the first name, etc.
         * This can be used to find the boundries of slices of columns.
         *
         * A NAME_BEGIN name compares as less than a non-empty name, meaning that you
         * can match the beginning of a slice with a ColumnKey with tailing NAME_BEGIN
         * names. Likewise, NAME_END names match the end of a slice.
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
                int comp = compareAt(o1.names[i], o2.names[i], i+1);
                if (comp != 0) return comp;
            }
            return 0;
        }
    }
}
