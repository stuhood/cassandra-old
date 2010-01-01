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
import org.apache.cassandra.db.Named;
import org.apache.cassandra.utils.ReducingIterator;

import org.apache.commons.collections.IteratorUtils;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;

/**
 * An immutable object which extends Slice to add serialized or realized columns.
 * At least 1 of the 2 will be set at a given time, and a missing value will be lazily
 * created.
 *
 * This laziness allows non-intersecting buffers to skip deserialization during (non-
 * major) compactions: intersecting buffers can be resolved into non-intersecting
 * buffers using merge().
 */
public class SliceBuffer extends Slice
{
    // either serialized and numCols must be set together...
    private DataOutputBuffer serialized = null;
    private int numCols = -1;

    // or realized must be set
    private List<Column> realized = null;

    public SliceBuffer(Slice.Metadata meta, ColumnKey key, ColumnKey end, Column... realized)
    {
        this(meta, key, end, Arrays.asList(realized));
    }

    public SliceBuffer(Slice.Metadata meta, ColumnKey key, ColumnKey end, List<Column> realized)
    {
        super(meta, key, end);
        assert realized != null;
        this.realized = Collections.unmodifiableList(realized);
    }

    public SliceBuffer(Slice.Metadata meta, ColumnKey key, ColumnKey end, int numCols, DataOutputBuffer serialized)
    {
        super(meta, key, end);
        assert serialized != null;
        assert numCols > -1;
        this.serialized = serialized;
        this.numCols = numCols;
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
     * @return One or more SliceBuffers resulting from the merge.
     */
    public static List<SliceBuffer> merge(ColumnKey.Comparator comparator, SliceBuffer one, SliceBuffer two)
    {
        assert comparator.compare(one.key, two.key, comparator.columnDepth()-1) == 0;
        NameComparator namecomp = new NameComparator(comparator);

        // mergesort and resolve the columns of the two slices
        Iterator<Column> co = IteratorUtils.collatedIterator(namecomp,
                                                             one.realized().iterator(),
                                                             two.realized().iterator());
        PeekingIterator<Column> merged = new ColumnResolvingIterator(co, namecomp);

        // build an ordered list of output Slices
        List<SliceBuffer> output = new LinkedList<SliceBuffer>();

        // left side of the overlap
        slicesFor(output, namecomp, merged, one.meta, two.meta, one.key, two.key);
        // overlap: resolved metadata, and max start and min end values
        Slice.Metadata olapmeta = Slice.Metadata.resolve(one.meta, two.meta);
        slicesFor(output, namecomp, merged, olapmeta, olapmeta,
                  comparator.max(one.key, two.key), comparator.min(one.end, two.end));
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
    private static void slicesFor(List<SliceBuffer> output, NameComparator namecomp, PeekingIterator<Column> iter, Slice.Metadata onemeta, Slice.Metadata twometa, ColumnKey onekey, ColumnKey twokey)
    {
        int comp = namecomp.compare(onekey, twokey);
        if (comp == 0)
            // slice would have 0 length
            return;
        Slice.Metadata meta;
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
        output.add(new SliceBuffer(meta, left, right, buff));
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

        if (meta.getLocalDeletionTime() < gcBefore && surviving == 0)
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

    /**
     * Comparator for Column names using a backing ColumnKey.Comparator.
     */
    static final class NameComparator implements Comparator<Named>
    {
        public final ColumnKey.Comparator ckcomp;
        public NameComparator(ColumnKey.Comparator ckcomp)
        {
            this.ckcomp = ckcomp;
        }
        
        public int compare(Named n1, Named n2)
        {
            return ckcomp.compareAt(n1.name(), n2.name(), ckcomp.columnDepth());
        }
    }

    /**
     * Resolves columns with equal names by comparing their priority.
     */
    static final class ColumnResolvingIterator extends ReducingIterator<Column, Column>
    {
        private NameComparator ncomp;
        private Column col = null;
        
        public ColumnResolvingIterator(Iterator<Column> source, NameComparator ncomp)
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
