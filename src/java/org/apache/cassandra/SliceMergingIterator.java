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

package org.apache.cassandra;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.utils.MergingIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;

import com.google.common.collect.Iterators;

public class SliceMergingIterator extends MergingIterator<ASlice, ASlice>
{
    private static Logger logger = Logger.getLogger(SliceMergingIterator.class);

    private final int depth;

    private final ColumnKey.Comparator comparator;
    private final SliceComparator scomparator;

    /**
     * Sorted list of Slices which are being prepared for return. As Slices are added
     * to the list, they are resolved against intersecting slices, resulting in a
     * buffer of non-intersecting slices.
     */
    private final LinkedList<ASlice> mergeBuff;

    // total input/output buffers
    private long inBufferCount = 0;
    private long outBufferCount = 0;
    // buffers that needed to be SB.merge()'d
    private long inMergeCount = 0;
    // buffers that resulted from SB.merge()
    private long outMergeCount = 0;

    @SuppressWarnings("unchecked")
    public SliceMergingIterator(List<Iterator> iterators, ColumnKey.Comparator comp)
    {
        super(getCollatingIterator(iterators, new SliceComparator(comp)));

        comparator = comp;
        scomparator = new SliceComparator(comparator);
        depth = comp.columnDepth();

        mergeBuff = new LinkedList<ASlice>();
    }

    @SuppressWarnings("unchecked")
    protected static CollatingIterator getCollatingIterator(List<Iterator> iterators, Comparator<? extends ASlice> comp)
    {
        CollatingIterator iter = new CollatingIterator(comp);
        for (Iterator asiter : iterators)
            iter.addIterator(asiter);
        return iter;
    }

    /**
     * Implements 'equals' in terms of intersection.
     */
    @Override
    protected boolean isEqual(ASlice sb1, ASlice sb2)
    {
        return scomparator.compare(sb1, sb2) == 0;
    }

    /**
     * Merges the given ASlice into the merge buffer.
     *
     * If a given slice intersects a slice already in the buffer, they are resolved
     * using ASlice.merge(), and the resulting slices are merged.
     *
     * FIXME: should probably just be insertion sort.
     */
    public void reduce(ASlice current)
    {
        inBufferCount++;
        ListIterator<ASlice> buffiter = mergeBuff.listIterator();
        Iterator<ASlice> rhsiter = Arrays.asList(current).iterator();

        ASlice buffcur = null;
        ASlice rhscur = null;
        while (true)
        {
            // ensure that items remain
            if (buffcur == null)
            {
                if (buffiter.hasNext())
                    buffcur = buffiter.next();
                else
                    break;
            }
            if (rhscur == null)
            {
                if (rhsiter.hasNext())
                    rhscur = rhsiter.next();
                else
                    break;
            }

            int comp = scomparator.compare(buffcur, rhscur);
            if (comp == 0)
            {
                // slices intersect: resolve and prepend to rhsiter
                List<ASlice> resolved = ASlice.merge(comparator,
                                                               buffcur, rhscur);
                rhsiter = Iterators.concat(resolved.iterator(), rhsiter);
                // buffcur and rhscur were consumed
                buffiter.remove();
                buffcur = null; rhscur = null;
                inMergeCount += 2; outMergeCount += resolved.size();
            }
            else if (comp > 0)
            {
                // insert rhscur before buffcur
                buffiter.set(rhscur);
                buffiter.add(buffcur);
                buffiter.previous();
                rhscur = null;
            }
            else
                // buffcur was the lesser: skip it
                buffcur = null;
        }

        // remaining items from rhsiter go to the end of the buffer
        if (rhscur != null)
            mergeBuff.add(rhscur);
        Iterators.addAll(mergeBuff, rhsiter);
    }

    /**
     * Empties the merge buffer.
     *
     * @return The next ASlices for this iterator.
     */
    @Override
    public Iterator<ASlice> getReduced()
    {
        DecoratedKey dkey = null; 
        ColumnFamily cf = null; 
        List<ASlice> toret = new LinkedList<ASlice>();
        for (ASlice slice : mergeBuff)
        {
            toret.add(slice);
            outBufferCount++;
        }
        mergeBuff.clear();
        return toret.iterator();
    }

    /**
     * Compares Slices using their key, but declares intersecting slices equal
     * so that we can resolve them.
     */
    protected static final class SliceComparator implements Comparator<ASlice>
    {
        private final ColumnKey.Comparator keycomp;
        public SliceComparator(ColumnKey.Comparator keycomp)
        {
            this.keycomp = keycomp;
        }
        
        public int compare(ASlice s1, ASlice s2)
        {
            if (keycomp.compare(s1.begin, s2.end) < 0 &&
                keycomp.compare(s2.begin, s1.end) < 0)
                // intersection
                return 0;
            return keycomp.compare(s1.begin, s2.begin);
        }
    }
}
