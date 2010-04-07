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

import com.google.common.base.Predicate;
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
        Metadata olapmeta = Metadata.resolve(one.meta, two.meta);
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

    /**
     * @return A copy of this buffer with tombstones removed, this exact buffer if no
     * changes were needed, or null if this buffer represented a metadata tombstone.
     */
    public ASlice garbageCollect(boolean major, int gcBefore)
    {
        if (!major)
            // garbage cannot be collected without a major compaction
            return this;

        // determine count of columns that will survive garbage collection
        SurvivorPredicate gcpred = new SurvivorPredicate(meta, gcBefore);
        int surviving = 0;
        for (Column col : columns())
            if (gcpred.apply(col))
                surviving++;

        if (meta.getLocalDeletionTime() < gcBefore && surviving == 0)
            // empty, and ready for gc
            return null;

        if (surviving == count())
            // all columns survived: return ourself without copying
            return this;

        // create a filtered copy
        List<Column> survivors = new ArrayList<Column>(surviving);
        Iterables.addAll(survivors, Iterables.filter(columns(), gcpred));
        return new Slice(meta, begin, end, survivors);
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
            begin.withName(ColumnKey.NAME_BEGIN).serialize(shared);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        if (count() == 0)
        {
            // for tombstones, only metadata is digested
            digest.update(shared.getData(), 0, shared.getLength());
            return;
        }
        for (Column col : columns())
        {
            digest.update(shared.getData(), 0, shared.getLength());
            col.updateDigest(digest);
        }
    }
    
    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<ASlice ").append(begin).append(" (");
        buff.append(meta).append(") ").append(end).append(">");
        return buff.toString();
    }

    /**
     * For use in garbage collection during major compactions.
     */
    static final class SurvivorPredicate implements Predicate<Column>
    {
        public final long parentMarkedForDeleteAt;
        public final int gcBefore;
        public SurvivorPredicate(Metadata meta, int gcBefore)
        {
            this.parentMarkedForDeleteAt = meta.getMarkedForDeleteAt();
            this.gcBefore = gcBefore;
        }

        /**
         * True if a Column should survive GC.
         */
        public boolean apply(Column col)
        {
            return !col.readyForGC(parentMarkedForDeleteAt, gcBefore);
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
