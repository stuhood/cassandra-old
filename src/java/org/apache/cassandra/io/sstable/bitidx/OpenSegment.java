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

import org.apache.cassandra.io.sstable.bitidx.avro.BinData;
import org.apache.cassandra.utils.obs.OpenBitSet;

/**
 * A deserialized segment for a particular bin: only the first 'numrows' bits in the bitset are valid.
 * Segments are comparable by their rowid.
 */
public final class OpenSegment implements Comparable<OpenSegment>
{
    public long rowid;
    public long numrows;
    public final OpenBitSet bitset;
    
    public OpenSegment(long initialSize)
    {
        this.bitset = new OpenBitSet(initialSize);
    }

    /**
     * Deep copies the given segment into this segment, expanding if necessary.
     */
    public void copy(OpenSegment that)
    {
        this.rowid = that.rowid;
        this.numrows = that.numrows;
        // expand if necessary
        this.bitset.ensureCapacityWords(that.bitset.getNumWords());
        // and copy valid words
        System.arraycopy(that.bitset.getBits(), 0, this.bitset.getBits(), 0, that.bitset.getNumWords());
    }

    void deserialize(BinData data)
    {
        rowid = data.rowid;
        numrows = data.numrows;

        // determine the number of valid 8-byte words in the serialized repr
        int words = Math.min(data.bits.limit() >> 3, OpenBitSet.bits2words(numrows));
        // deserialize bits: only the first 'numrows' bits will be valid
        bitset.ensureCapacityWords(words);
        data.bits.asLongBuffer().get(bitset.getBits(), 0, words);
    }
    
    public int compareTo(OpenSegment that)
    {
        if (this.rowid < that.rowid)
            return -1;
        if (this.rowid > that.rowid)
            return 1;
        return 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof OpenSegment))
            return false;
        OpenSegment that = (OpenSegment)o;
        return this.rowid == that.rowid;
    }

    @Override
    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<Segment ").append(rowid).append(' ');
        buff.append(bitset.cardinality()).append('/').append(numrows).append('>');
        return buff.toString();
    }
}
