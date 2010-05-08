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

package org.apache.cassandra;

import java.io.*;
import java.util.*;
import java.security.MessageDigest;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.db.Named;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ReducingIterator;

import org.apache.commons.collections.IteratorUtils;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;

/**
 * An immutable object representing a Slice: a Slice is a sorted sequence
 * of columns beginning at begin (inclusive) and ending at end (exclusive) that
 * share the same parents, and the same Metadata.
 *
 * The Metadata in a Slice affects any columns between begin, inclusive, and end,
 * exclusive. But if it is acting as a tombstone, a Slice may not contain any columns.
 */
public abstract class ASlice
{
    public final Metadata meta;
    // inclusive beginning of our range: all but the last name will be equal for
    // columns in the slice
    public final ColumnKey begin;
    // exclusive end to our range
    public final ColumnKey end;

    /**
     * @param meta Metadata for the key range this Slice defines.
     * @param begin The key for the first column in the Slice.
     * @param end A key greater than the last column in the Slice.
     */
    public ASlice(Metadata meta, ColumnKey begin, ColumnKey end)
    {
        assert meta != null;
        assert begin != null && end != null;
        this.meta = meta;
        this.begin = begin;
        this.end = end;
    }

    /**
     * @return The number of columns in this Slice, without modifying internal state.
     */
    public abstract int count();

    /**
     * @return An immutable sorted list of columns in this Slice.
     */
    public abstract List<Column> columns();

    /**
     * Merges the given intersecting buffers. The output will be one or more non-
     * intersecting slice buffers (depending on size/key/metadata) in sorted order by
     * key.
     *
     * This method asserts that the Slices have the same parents, meaning that they
     * might intersect/overlap one another (otherwise, the output would be exactly the
     * same as the input).
     *
     * TODO: This method does 3n comparisons:
     * 1. CollatingIterator merge sorts,
     * 2. ReducingIterator merges consecutive equal values,
     * 3. Slice collection compares ranges.
     * Modifying ReducingIterator to perform collation, and moving Slice creation
     * logic into the ReducingIterator subclass would cut this down to less than
     * 2 comparisons.
     *
     * FIXME: Merge consecutive slices with equal metadata by calling slicesFor once
     *
     * @return One or more Slices resulting from the merge.
     */
    public static List<ASlice> merge(ColumnKey.Comparator comparator, ASlice one, ASlice two)
    {
        assert comparator.compare(one.begin, two.begin, comparator.columnDepth()-1) == 0;
        Named.Comparator namecomp = new Named.Comparator(comparator);

        // mergesort and resolve the columns of the two slices
        Iterator<Column> co = IteratorUtils.collatedIterator(namecomp,
                                                             one.columns().iterator(),
                                                             two.columns().iterator());
        PeekingIterator<Column> merged = new ColumnResolvingIterator(co, namecomp);

        // build an ordered list of output Slices
        List<ASlice> output = new LinkedList<ASlice>();

        // left side of the overlap
        slicesFor(output, namecomp, merged, one.meta, two.meta, one.begin, two.begin);
        // overlap: resolved metadata, and max start and min end values
        Metadata olapmeta = Metadata.max(one.meta, two.meta);
        slicesFor(output, namecomp, merged, olapmeta, olapmeta,
                  comparator.max(one.begin, two.begin), comparator.min(one.end, two.end));
        // right side of the overlap
        slicesFor(output, namecomp, merged, two.meta, one.meta, one.end, two.end);

        assert !merged.hasNext();
        return output;
    }

    /**
     * Adds zero or more Slices of Columns which fall between the min and max input keys
     * from the given iterator to the given output list.
     * FIXME: Add size restrictions based on SSTable.SLICE_MAX
     */
    private static void slicesFor(List<ASlice> output, Named.Comparator namecomp, PeekingIterator<Column> iter, Metadata onemeta, Metadata twometa, ColumnKey onekey, ColumnKey twokey)
    {
        int comp = namecomp.compare(onekey, twokey);
        if (comp == 0)
            // slice would have 0 length
            return;
        Metadata meta;
        ColumnKey left,right;
        if (comp < 0)
        {
            meta = onemeta;
            left = onekey;
            right = twokey;
        }
        else
        {
            meta = twometa;
            left = twokey;
            right = onekey;
        }

        List<Column> buff = new ArrayList<Column>();
        while (iter.hasNext() && namecomp.compare(iter.peek(), right) < 0)
            buff.add(iter.next());
        output.add(new Slice(meta, left, right, buff));
    }
    
    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<ASlice ").append(begin).append(" (");
        buff.append("count=").append(count()).append(meta);
        return buff.append(") ").append(end).append(">").toString();
    }

    /**
     * Metadata shared between columns in a Slice: currently only contains deletion info.
     */
    public static final class Metadata
    {
        public final long markedForDeleteAt;
        public final int localDeletionTime;

        public Metadata()
        {
            this(Long.MIN_VALUE, Integer.MIN_VALUE);
        }

        public Metadata(long markedForDeleteAt, int localDeletionTime)
        {
            this.markedForDeleteAt = markedForDeleteAt;
            this.localDeletionTime = localDeletionTime;
        }

        /**
         * @return This Metadata object, or a new one representing the composite of the input metadata.
         */
        public Metadata max(long markedForDeleteAt, int localDeletionTime)
        {
            if (this.markedForDeleteAt == markedForDeleteAt && this.localDeletionTime == localDeletionTime)
                // identical
                return this;
            return new Metadata(Math.max(this.markedForDeleteAt, markedForDeleteAt),
                                Math.max(this.localDeletionTime, localDeletionTime));
        }

        /**
         * @return A Metadata object representing the composite of the input metadata.
         */
        public static Metadata max(Metadata one, Metadata two)
        {
            return one.max(two.markedForDeleteAt, two.localDeletionTime);
        }

        /**
         * @param gcBefore The local (server) time before which tombstone metadata can be removed.
         * @return True if this Metadata is not marked for deletion, or if the deletion occurred long enough ago to gc.
         */
        boolean readyForGC(int gcBefore)
        {
            return !isMarkedForDelete() || localDeletionTime <= gcBefore;
        }

        /**
         * @return True is this Metadata represents a tombstone.
         */
        public boolean isMarkedForDelete()
        {
            return markedForDeleteAt != Long.MIN_VALUE;
        }

        @Override
        public String toString()
        {
            return isMarkedForDelete() ? " " + markedForDeleteAt + "$" + localDeletionTime : "";
        }
    }

    /**
     * Transforms an input Slice into a copy with tombstones removed, the original object if no
     * changes were needed, or null if the Slice represented a sufficiently old metadata tombstone.
     */
    public static class GCFunction implements Function<ASlice, ASlice>
    {
        public final int gcBefore;
        public GCFunction(int gcBefore)
        {
            this.gcBefore = gcBefore;
        }

        public ASlice apply(ASlice slice)
        {
            if (gcBefore == Integer.MIN_VALUE)
                // garbage cannot be collected without a major compaction
                return slice;

            // determine count of columns that will survive garbage collection
            int surviving = 0;
            for (Column col : slice.columns())
                if (!col.readyForGC(slice.meta, gcBefore))
                    surviving++;

            if (surviving == 0 && slice.meta.readyForGC(gcBefore))
                // empty, and ready for gc
                return null;

            if (surviving == slice.count())
                // all columns survived: return ourself without copying
                return slice;

            // create a filtered copy
            List<Column> survivors = new ArrayList<Column>(surviving);
            for (Column col : slice.columns())
                if (!col.readyForGC(slice.meta, gcBefore))
                    survivors.add(col);
            return new Slice(slice.meta, slice.begin, slice.end, survivors);
        }
    }

    /**
     * Resolves columns with equal names by comparing their priority.
     */
    static final class ColumnResolvingIterator extends ReducingIterator<Column, Column>
    {
        private Named.Comparator ncomp;
        private Column col = null;
        
        public ColumnResolvingIterator(Iterator<Column> source, Named.Comparator ncomp)
        {
            super(source);
            this.ncomp = ncomp;
        }

        @Override
        public boolean isEqual(Column c1, Column c2)
        {
            return ncomp.compare(c1, c2) == 0;
        }

        @Override
        public void reduce(Column newcol)
        {
            if (col == null || col.comparePriority(newcol) < 0)
                col = newcol;
        }

        @Override
        public Column getReduced()
        {
            Column reduced = col;
            col = null;
            return reduced;
        }
    }
}
