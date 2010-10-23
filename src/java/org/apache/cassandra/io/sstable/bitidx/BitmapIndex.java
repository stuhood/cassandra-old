/**
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
 */

package org.apache.cassandra.io.sstable.bitidx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.apache.cassandra.utils.Pair;

public class BitmapIndex
{
    // file metadata is embedded as a serialized object using Avro's metadata system
    final static String META_KEY = "index_metadata";

    // TODO: choose less arbitrary value
    public final static int MAX_BINS = 32;

    /** The target number of bits in a data segment, and a rounded max size in bytes. */
    final static int SEGMENT_SIZE_BITS = 32000;
    final static int SEGMENT_SIZE_BYTES = 1 + (SEGMENT_SIZE_BITS / 8);

    // threadlocal singleton for looking up values in bins
    final ThreadLocal<Binnable> BINNABLE_SINGLETON = new ThreadLocal<Binnable>()
    {
        @Override
        public Binnable initialValue()
        {
            return new Binnable(null);
        }
    };

    /*************************************************************************/

    public final ColumnDefinition cdef;
    public final Descriptor desc;
    public final Component component;

    protected final BinComparator comparator;
    protected final ArrayList<Binnable> bins;

    protected BitmapIndex(Descriptor desc, Component component, ColumnDefinition cdef)
    {
        this.desc = desc;
        this.component = component;
        this.cdef = cdef;
        this.comparator = new BinComparator(cdef.validator);
        this.bins = new ArrayList<Binnable>(BitmapIndex.MAX_BINS);
    }

    public ByteBuffer name()
    {
        return cdef.name;
    }

    protected int floor(ByteBuffer v)
    {
        int idx = Collections.binarySearch(bins, binnable(v), comparator);
        if (idx < 0)
            // round down to previous bin (or -1)
            return -(2 + idx);
        // exact match
        return idx;
    }

    /**
     * An int representing the relative position of the given value to the given bin number.
     * Results >= 0 indicate that the value is contained by the bin.
     * @return -2 if (v < min), -1 if (v > max), 0 if (v == min), 1 if (min < v < max), 2 if (v == max).
     */
    protected final int match(int binidx, ByteBuffer v)
    {
        Binnable bin = bins.get(binidx);
        int c = cdef.validator.compare(bin.min, v);
        if (c > 0)
            // less than min
            return -2;
        if (c == 0)
            // exact match for min
            return 0;
        // else, v > min
        
        if (bin.max.remaining() == 0)
            // greater than single valued bin
            return -1;
        // else, is ranged bin
        c = cdef.validator.compare(v, bin.max);
        if (c < 0)
            // falls into ranged bin
            return 1;
        if (c == 0)
            // exact match for max
            return 2;
        // else, v > max
        return -1;
    }

    private final Binnable binnable(ByteBuffer value)
    {
        Binnable singleton = BINNABLE_SINGLETON.get();
        singleton.min = value;
        return singleton;
    }

    @Override
    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<").append(this.getClass().getSimpleName());
        buff.append(" [").append(cdef.getIndexName()).append(']');
        buff.append(" on ").append(desc.filenameFor(component));
        return buff.append('>').toString();
    }

    /**
     * Comparable superclass for bin lookup.
     */
    static class Binnable
    {
        public ByteBuffer min;
        public ByteBuffer max;
        public Binnable(ByteBuffer min)
        {
            this(min, FBUtilities.EMPTY_BYTE_BUFFER);
        }

        public Binnable(ByteBuffer min, ByteBuffer max)
        {
            this.min = min;
            this.max = max;
        }

        public String toString(AbstractType type)
        {
            StringBuilder buff = new StringBuilder();
            buff.append("#<Bin ").append(type.getString(min)).append(',');
            return buff.append(type.getString(max)).append('>').toString();
        }
    }

    /**
     * Bins are only compared using their min value, so a set of bins must not intersect.
     */
    static final class BinComparator implements java.util.Comparator<Binnable>
    {
        public final AbstractType type;
        public BinComparator(AbstractType type)
        {
            this.type = type;
        }

        @Override
        public int compare(Binnable o1, Binnable o2)
        {
            return type.compare(o1.min, o2.min);
        }
    }
}
