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

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.utils.TransformingIterator;

import com.google.common.collect.Iterators;

/**
 * Collates and merges Slices from multiple sources.
 */
public class MergingScanner extends TransformingIterator<ASlice, ASlice> implements Scanner
{
    private static Logger logger = Logger.getLogger(MergingScanner.class);

    private final List<Scanner> scanners;
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
    protected long inBufferCount = 0;
    protected long outBufferCount = 0;
    // buffers that needed to be SB.merge()'d
    protected long inMergeCount = 0;
    // buffers that resulted from SB.merge()
    protected long outMergeCount = 0;

    @SuppressWarnings("unchecked")
    public MergingScanner(List<Scanner> scanners, ColumnKey.Comparator comp)
    {
        super(getCollatingIterator(scanners, new SliceComparator(comp)));

        this.scanners = scanners;
        comparator = comp;
        scomparator = new SliceComparator(comparator);
        depth = comp.columnDepth();

        mergeBuff = new LinkedList<ASlice>();
    }

    @SuppressWarnings("unchecked")
    protected static CollatingIterator getCollatingIterator(List<Scanner> scanners, Comparator<? extends ASlice> comp)
    {
        CollatingIterator iter = new CollatingIterator(comp);
        for (Scanner scanner : scanners)
            iter.addIterator(scanner);
        return iter;
    }

    @Override
    public ColumnKey.Comparator comparator()
    {
        return comparator;
    }

    /**
     * Inserts the given ASlice into the merge buffer.
     *
     * If a given slice intersects a slice already in the buffer, they are resolved using ASlice.merge(), ensuring that
     * the contents remain non-intersecting.
     */
    public boolean transform(ASlice input)
    {
        if (!mergeBuff.isEmpty() && !scomparator.intersects(mergeBuff.getFirst(), input))
            // input slice does not intersect with the next transformed slice
            return false;
            
        inBufferCount++;

        // insertion sort
        ListIterator<ASlice> buffiter = mergeBuff.listIterator();
        while (buffiter.hasNext())
        {
            ASlice buffcur = buffiter.next();

            int comp = scomparator.compare(buffcur, input);
            if (comp == 0)
            {
                // slices intersect: resolve
                List<ASlice> resolved = ASlice.merge(comparator, buffcur, input);
                // buffcur was consumed: replace with resolved slices
                buffiter.remove();
                for (ASlice resslice : resolved)
                    buffiter.add(resslice);
                inMergeCount += 2; outMergeCount += resolved.size();
                return true;
            }
            else if (comp > 0)
            {
                // insert input before buffcur
                buffiter.set(input);
                buffiter.add(buffcur);
                return true;
            }
        }

        // else, input is larger than all other entries
        mergeBuff.add(input);
        return true;
    }

    /**
     * Pop the first slice from the merge buffer: may return null if the slice was completely GC'd.
     *
     * @return The ASlices for this iterator.
     */
    @Override
    public ASlice transformed()
    {
        outBufferCount++;
        return mergeBuff.pop();
    }

    @Override
    public boolean complete()
    {
        return !mergeBuff.isEmpty();
    }

    /**
     * Not thread safe! CompactionScanner wraps calls to this method to provide thread-safe progress.
     */
    @Override
    public long getBytesRemaining()
    {
        long remaining = 0;
        for (Scanner scanner : scanners)
            remaining += scanner.getBytesRemaining();
        return remaining;
    }

    @Override
    public void close() throws IOException
    {
        IOException e = null;
        for (Scanner scanner : scanners)
        {
            try
            {
                scanner.close();
            }
            catch (IOException ie)
            {
                e = ie;
            }
        }

        // we can only rethrow one exception, but we want to close all scanners
        if (e != null)
            throw e;
    }

    /**
     * Compares Slices using their key, and provides an intersection method.
     */
    protected static final class SliceComparator implements Comparator<ASlice>
    {
        private final ColumnKey.Comparator keycomp;
        public SliceComparator(ColumnKey.Comparator keycomp)
        {
            this.keycomp = keycomp;
        }
        
        public boolean intersects(ASlice s1, ASlice s2)
        {
            return keycomp.compare(s1.begin, s2.end) <= 0 && keycomp.compare(s2.begin, s1.end) <= 0;
        }

        @Override
        public int compare(ASlice s1, ASlice s2)
        {
            return keycomp.compare(s1.begin, s2.begin);
        }
    }
}
