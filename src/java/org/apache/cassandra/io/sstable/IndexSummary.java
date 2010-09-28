package org.apache.cassandra.io.sstable;
/*
 * 
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
 * 
 */


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.Pair;

/**
 * Two approaches to building an IndexSummary:
 * 1. Call maybeAddEntry with every potential index entry
 * 2. Call shouldAddEntry, [addEntry,] incrementRowid
 */
public class IndexSummary
{
    private ArrayList<KeyPosition> indexPositions;
    private long keysWritten = 0;

    public IndexSummary(long expectedKeys)
    {
        long expectedEntries = expectedKeys / DatabaseDescriptor.getIndexInterval();
        if (expectedEntries > Integer.MAX_VALUE)
            // TODO: that's a _lot_ of keys, or a very low interval
            throw new RuntimeException("Cannot use index_interval of " + DatabaseDescriptor.getIndexInterval() + " with " + expectedKeys + " (expected) keys.");
        indexPositions = new ArrayList<KeyPosition>((int)expectedEntries);
    }

    public void incrementRowid()
    {
        keysWritten++;
    }

    public boolean shouldAddEntry()
    {
        return keysWritten % DatabaseDescriptor.getIndexInterval() == 0;
    }

    public void addEntry(DecoratedKey decoratedKey, long indexPosition)
    {
        indexPositions.add(new KeyPosition(decoratedKey, indexPosition));
    }

    public void maybeAddEntry(DecoratedKey decoratedKey, long indexPosition)
    {
        if (shouldAddEntry())
            addEntry(decoratedKey, indexPosition);
        incrementRowid();
    }

    public List<KeyPosition> getIndexPositions()
    {
        return indexPositions;
    }

    public void complete()
    {
        indexPositions.trimToSize();
    }

    /**
     * @return The offset in indexPositions of the first key less than or equal to dk, or -1.
     */
    private int binarySearch(DecoratedKey dk)
    {
        int index = Collections.binarySearch(indexPositions, new KeyPosition(dk, -1));
        if (index >= 0)
            // exact match
            return index;
        // binary search gives us the first index _greater_ than the key searched for,
        // i.e., its insertion position
        int greaterThan = (index + 1) * -1;
        if (greaterThan == 0)
            return -1;
        return greaterThan - 1;
    }

    /**
     * @return The position in the index file to start scanning to find the given key
     * (at most indexInterval keys away)
     */
    KeyPosition getIndexScanPosition(DecoratedKey decoratedKey)
    {
        int index = binarySearch(decoratedKey);
        if (index == -1)
            // given key is before the beginning of the file
            return null;
        return indexPositions.get(index);
    }

    /**
     * @return The position in the index file to start scanning to find the given rowid
     * (at most indexInterval keys away), and the rowid for the position.
     */
    Pair<KeyPosition,Long> getIndexScanPosition(long rowid)
    {
        int index = (int)(rowid / DatabaseDescriptor.getIndexInterval());
        return new Pair<KeyPosition,Long>(indexPositions.get(index),
                                          // round down to the matched rowid
                                          DatabaseDescriptor.getIndexInterval() * (long)index);
    }

    /**
     * @param range An unwrapped AbstractBounds (rhs may either be greater than lhs, or equal to the minimum token).
     * TODO: Add a method to AbstractBounds that lets us assert that it is unwrapped.
     * @return The approximate min and max rowids (within INDEX_INTERVAL) matched by the given AbstractBound, or null
     * if no rows match. Note that because of rounding, the maximum rowid may be past the end of the file.
     */
    Pair<Long,Long> getRowidRange(AbstractBounds range)
    {
        // max (rounded up)
        int maxIndex = binarySearch(new DecoratedKey(range.right, null));
        if (maxIndex == -1 && Range.isWrapAround(range.left, range.right))
            // wrapping range: rhs is "infinite"
            maxIndex = indexPositions.size();
        else if (maxIndex == -1)
            // entire range falls before the beginning of the file
            return null;
        else
            // round up to next position
            maxIndex += 1;
        long maxRowid = maxIndex * DatabaseDescriptor.getIndexInterval();

        // min (already rounded down by summary)
        int minIndex = binarySearch(new DecoratedKey(range.left, null));
        long minRowid = minIndex == -1 ? 0 : minIndex * DatabaseDescriptor.getIndexInterval();

        return new Pair<Long,Long>(minRowid, maxRowid);
    }

    /**
     * This is a simple container for the index Key and its corresponding position
     * in the index file. Binary search is performed on a list of these objects
     * to find where to start looking for the index entry containing the data position
     * (which will be turned into a PositionSize object)
     */
    public static final class KeyPosition implements Comparable<KeyPosition>
    {
        public final DecoratedKey key;
        public final long indexPosition;

        public KeyPosition(DecoratedKey key, long indexPosition)
        {
            this.key = key;
            this.indexPosition = indexPosition;
        }

        public int compareTo(KeyPosition kp)
        {
            return key.compareTo(kp.key);
        }

        public String toString()
        {
            return key + ":" + indexPosition;
        }
    }
}
