package org.apache.cassandra.io;
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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.security.MessageDigest;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

/**
 * Immutable object which extends Slice to add serialized or realized columns.
 * At least 1 of the 2 will be set at a given time, and a missing value will be
 * lazily created.
 */
public class SliceBuffer extends Slice
{
    // either serialized and numCols must be set together...
    private DataOutputBuffer serialized = null;
    private int numCols = -1;

    // or realized must be set
    private List<Column> realized = null;

    SliceBuffer(Slice.Metadata meta, ColumnKey key, ColumnKey nextKey, List<Column> realized)
    {
        super(meta, key, nextKey);
        assert this.realized != null;
        this.realized = Collections.unmodifiableList(realized);
    }

    SliceBuffer(Slice.Metadata meta, ColumnKey key, ColumnKey nextKey, int numCols, DataOutputBuffer serialized)
    {
        super(meta, key, nextKey);
        assert this.serialized != null;
        this.serialized = serialized;
        numCols = numCols;
    }

    public DataOutputBuffer serialized()
    {
        if (serialized != null)
            return serialized;
        
        // serialize the columns
        serialized = new DataOutputBuffer(realized.size() * 10);
        for (Column col : realized)
            Column.serializer().serialize(col, serialized);
        return serialized;
    }

    /**
     * An immutable sorted list of columns for this buffer.
     */
    public List<Column> realized()
    {
        if (realized != null)
            return realized;

        // realize the columns from the buffer
        Column[] cols = new Column[numCols];
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(serialized.getData(),
                                                                              0, serialized.getLength()));
        try
        {
            for (int i = 0; i < cols.length; i++)
                cols[i] = (Column)Column.serializer().deserialize(stream);
            realized = Arrays.asList(cols);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        return realized;
    }

    /**
     * Merges the given intersecting buffers. The output will be 1 or more non-
     * intersecting slice buffers (depending on size/key/metadata) in sorted order by
     * key.
     *
     * This method can only be used when the Slices intersect/overlap one another,
     * meaning that they have the same parents (otherwise, the output would be
     * exactly the same as the input).
     *
     * TODO: Use Collating/Reducing iterators for this merge, since it is traditional
     *
     * @return One or more SliceBuffers resulting from the merge.
     */
    public static List<SliceBuffer> merge(ColumnKey.Comparator comparator, SliceBuffer left, SliceBuffer right)
    {
        if (comparator.compare(left.key, right.key) > 0)
        {
            // left should have the <= key
            SliceBuffer swap = left;
            left = right;
            right = swap;
        }

        final int cdepth = comparator.columnDepth();
        final List<SliceBuffer> output = new LinkedList<SliceBuffer>();

        List<Column> leftover;
        if (comparator.compare(left.key, right.key) < 0)
        {
            // find the beginning of the overlap in the left buffer
            // TODO: could use binary search here
            int idx = 0;
            List<Column> lcols = left.realized();
            for (; idx < lcols.size(); idx++)
                if (comparator.compareAt(lcols.get(idx).name(),
                                         right.key.name(cdepth), cdepth) > 0)
                    break;
            
            // add a truncated copy of the left buffer to the output
            output.add(new SliceBuffer(left.meta, left.key, right.key,
                                       lcols.subList(0, idx)));
            leftover = lcols.subList(idx, lcols.size());
        }
        else
            // overlap begins at the beginning of the left buffer
            leftover = left.realized();


        // while columns are less than left.end, merge sort into an overlap buffer
        List<Column> overlap = new ArrayList<Column>();
        Iterator<Column> liter = leftover.iterator();
        Iterator<Column> riter = right.realized().iterator();
        Column lcol = liter.hasNext() ? liter.next() : null;
        Column rcol = riter.hasNext() ? riter.next() : null;
        while (lcol != null && rcol != null)
        {
            int comp = comparator.compareAt(lcol.name(), rcol.name(), cdepth);
            if (comp == 0)
            {
                // resolve and add
                overlap.add(lcol.comparePriority(rcol) <= 0 ? rcol : lcol);
                lcol = liter.hasNext() ? liter.next() : null;
                rcol = riter.hasNext() ? riter.next() : null;
            }
            else if (comp < 0)
            {
                overlap.add(lcol);
                lcol = liter.hasNext() ? liter.next() : null;
            }
            else // comp > 0
            {
                overlap.add(rcol);
                rcol = riter.hasNext() ? riter.next() : null;
            }
        }
        output.add(new SliceBuffer(Metadata.resolve(left.meta, right.meta),
                                   right.key,
                                   Ordering.from(comparator).min(left.end, right.end),
                                   overlap));
       

        if (lcol != null)
        {
            // if columns remain in left output, output with left metadata
            List<Column> remainder = new ArrayList<Column>();
            remainder.add(lcol);
            Iterators.addAll(remainder, liter);
            output.add(new SliceBuffer(left.meta, right.end, left.end, remainder));
        }
        else if (rcol != null)
        {
            // if columns remain in right output, output with right metadata
            List<Column> remainder = new ArrayList<Column>();
            remainder.add(rcol);
            Iterators.addAll(remainder, riter);
            output.add(new SliceBuffer(left.meta, left.end, right.end, remainder));
        }

        // TODO: split any output slices larger than TARGET_MAX_SLICE_BYTES
        return output;
    }

    /**
     * @return A copy of this buffer with tombstones removed, this exact buffer if no
     * changes were needed, or null if this buffer represented a metadata tombstone.
     */
    public SliceBuffer garbageCollect(boolean major, int gcBefore)
    {
        if (!major)
            // garbage cannot be collected without a major compaction
            return this;

        // determine count of columns that will survive garbage collection
        SurvivorPredicate gcpred = new SurvivorPredicate(meta, gcBefore);
        int surviving = Iterables.frequency(realized(), gcpred);

        if (meta.getLocalDeletionTime() > gcBefore && surviving == 0)
            // empty, and ready for gc
            return null;

        if (surviving == realized.size())
            // all columns survived: return ourself without copying
            return this;

        // create a filtered copy
        List<Column> survivors = new ArrayList<Column>(surviving);
        Iterables.addAll(survivors, Iterables.filter(realized(), gcpred));
        return new SliceBuffer(meta, key, end, survivors);
    }

    /**
     * For each column, adds the parent portion of the key, the metadata and the
     * content of the column to the given digest.
     *
     * An empty slice (acting as a tombstone), will digest only the key and metadata.
     */
    public void updateDigest(MessageDigest digest)
    {
        // parent data that is shared for these columns
        DataOutputBuffer shared = new DataOutputBuffer();
        try
        {
            meta.serialize(shared);
            key.withName(ColumnKey.NAME_BEGIN).serialize(shared);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        if (numCols == 0)
            // for tombstones, only metadata is digested
            digest.update(shared.getData(), 0, shared.getLength());
        for (Column col : realized())
        {
            digest.update(shared.getData(), 0, shared.getLength());
            col.updateDigest(digest);
        }
    }
    
    /**
     * For use during major compactions (when GC is applicable).
     */
    static final class SurvivorPredicate implements Predicate<Column>
    {
        public final long parentMarkedForDeleteAt;
        public final int gcBefore;
        public SurvivorPredicate(Slice.Metadata meta, int gcBefore)
        {
            this.parentMarkedForDeleteAt = meta.getMarkedForDeleteAt();
            this.gcBefore = gcBefore;
        }

        public boolean apply(Column col)
        {
            return !col.readyForGC(parentMarkedForDeleteAt, gcBefore);
        }
    }
}
