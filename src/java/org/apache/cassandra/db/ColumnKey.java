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
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Ordering;

/**
 * The full path to a column or slice boundary in a column family.
 */
public class ColumnKey implements Named
{
    // singleton names representing the beginning and end of a subrange
    public static final byte[] NAME_BEGIN = new byte[0];
    public static final byte[] NAME_END = null;

    public final DecoratedKey dk;
    public final byte[][] names;

    /**
     * Creates a column key of the given depth, with the given names. Any names
     * not specified will be NAME_BEGIN.
     */
    public ColumnKey(DecoratedKey dk, int depth)
    {
        assert 0 < depth && depth < Byte.MAX_VALUE;
        this.dk = dk;
        this.names = new byte[depth][];
        for (int i = 0; i < depth; i++)
            // remaining names as NAME_BEGIN
            this.names[i] = NAME_BEGIN;
    }

    public ColumnKey(DecoratedKey dk, byte[]... names)
    {
        assert names.length < Byte.MAX_VALUE;
        this.dk = dk;
        this.names = names;
    }

    /**
     * @return The name at the maximum/column depth.
     */
    public byte[] name()
    {
        return names[names.length-1];
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
        if(ColumnFamilyType.Super == DatabaseDescriptor.getColumnFamilyType(table, cf))
            return new Comparator(DatabaseDescriptor.getComparator(table, cf),
                                  DatabaseDescriptor.getSubComparator(table, cf));
        return new Comparator(DatabaseDescriptor.getComparator(table, cf));
    }

    @Override
    public boolean equals(Object o)
    {
        throw new RuntimeException("Use ColumnKey.Comparator");
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(new int[]{dk.hashCode(), Arrays.hashCode(names)});
    }

    /**
     * A Comparator that supports comparing ColumnKeys at an arbitrary depth.
     * The implementation of java.util.Comparator.compare() uses the maximum
     * depth.
     *
     * FIXME: This comparator wraps null safety around AbstractType, but we should
     * push it down instead.
     */
    public static class Comparator extends Ordering<ColumnKey>
    {
        private final AbstractType[] nameComparators;
        public Comparator(AbstractType... nameComparators)
        {
            this.nameComparators = nameComparators;
        }

        public int columnDepth()
        {
            return nameComparators.length;
        }

        /**
         * TODO: Creates new comparator per-call: see FIXME on class.
         */
        public java.util.Comparator<byte[]> comparatorAt(int depth)
        {
            if (depth == -1)
                depth = nameComparators.length;
            return Ordering.from(nameComparators[depth-1]).nullsLast();
        }

        public AbstractType typeAt(int depth)
        {
            return nameComparators[depth-1];
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
         * Implement Ordering/Comparator.compare() by comparing at the maximum depth.
         */
        @Override
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

        private String getString(int namepos, byte[] name)
        {
            return name == null ? "null" : nameComparators[namepos].getString(name);
        }

        public String getString(ColumnKey key)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("#<ColumnKey [");
            sb.append(key.dk).append(",");
            sb.append("\"").append(getString(0, key.names[0])).append("\"");
            for (int i = 1; i < key.names.length; i++)
                sb.append(",\"").append(getString(i, key.names[i])).append("\"");
            sb.append("]>");
            return sb.toString();
        }
    }
}
