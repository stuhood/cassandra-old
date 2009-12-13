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
    // FIXME: safer structure
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
     *
     * TODO: confirm that AbstractTypes can handle null comparisons.
     */
    public static Comparator<ColumnKey> getComparator(String table, String cf)
    {
        final AbstractType[] nameComparators = new AbstractType[]{
            DatabaseDescriptor.getComparator(table, cf),
            DatabaseDescriptor.getSubComparator(table, cf)};
        // TODO: add caching of comparators for CFs
        return new Comparator<ColumnKey>()
        {
            public int compare(ColumnKey o1, ColumnKey o2)
            {
                assert o1.names.length == o2.names.length;
                int comp = o1.key.compareTo(o2.key);
                if (comp != 0)
                    return comp;
                for (int i = 0; i < o1.names.length; i++)
                {
                    comp = nameComparators[i].compare(o1.names[i], o2.names[i]);
                    if (comp != 0)
                        return comp;
                }
                return 0;
            }
        };
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
}
