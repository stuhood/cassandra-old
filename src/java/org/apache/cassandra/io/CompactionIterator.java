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


import java.io.Closeable;
import java.io.IOException;
import java.io.IOError;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.Scanner;
import org.apache.cassandra.io.util.DataOutputBuffer;

import com.google.common.collect.Iterators;

public class CompactionIterator extends ReducingIterator<SliceBuffer, SliceBuffer> implements Closeable
{
    private static Logger logger = Logger.getLogger(CompactionIterator.class);

    // shared file buffer size for all input SSTables.
    public static final int TOTAL_FILE_BUFFER_BYTES = 1 << 22;

    private final int gcBefore;
    private final boolean major;
    private final int depth;

    /**
     * The comparators for all SSTables we are compacting.
     */
    private final ColumnKey.Comparator comparator;
    private final SliceComparator scomparator;

    /**
     * Sorted list of Slices which are being prepared for return. As Slices are added
     * to the list, they are resolved against intersecting slices, resulting in a
     * buffer of non-intersecting slices.
     *
     * NB: This buffer is the source of the majority of memory usage for compactions.
     * Its maximum size in bytes is roughly equal to:
     * (CompactionManager.maxCompactThreshold * SSTable.TARGET_MAX_SLICE_BYTES)
     */
    private final LinkedList<SliceBuffer> mergeBuff;

    private final ColumnFamily emptycf;

    // total input/output buffers to compaction
    private long inBufferCount = 0;
    private long outBufferCount = 0;
    // buffers that needed to be SB.merge()'d
    private long inMergeCount = 0;
    // buffers that resulted from SB.merge()
    private long outMergeCount = 0;
    // buffers garbage collected
    private long gcCount = 0;

    // total bytes in all input scanners
    private final long totalBytes;
    // bytes remaining in input scanners
    private final AtomicLong bytesRemaining = new AtomicLong();

    public CompactionIterator(Collection<SSTableReader> sstables, int gcBefore, boolean major) throws IOException
    {
        this(sstables, getCollatingIterator(sstables, new SliceComparator(sstables.iterator().next().getComparator())), gcBefore, major);
    }

    @SuppressWarnings("unchecked")
    protected CompactionIterator(Collection<SSTableReader> sstables, CollatingIterator iter, int gcBefore, boolean major) throws IOException
    {
        super(iter);
        this.gcBefore = gcBefore;
        this.major = major;

        // fields shared for all sstables
        SSTableReader head = sstables.iterator().next();
        comparator = head.getComparator();
        scomparator = new SliceComparator(comparator);
        emptycf = head.makeColumnFamily();
        depth = head.getColumnDepth();

        // open all scanners
        updateBytesRemaining();
        totalBytes = bytesRemaining.get();

        mergeBuff = new LinkedList<SliceBuffer>();
    }

    protected static CollatingIterator getCollatingIterator(Collection<SSTableReader> sstables, Comparator<SliceBuffer> comp) throws IOException
    {
        CollatingIterator iter = FBUtilities.<SliceBuffer>getCollatingIterator(comp);
        int bufferPer = TOTAL_FILE_BUFFER_BYTES / sstables.size();
        for (SSTableReader sstable : sstables)
        {
            Scanner scanner = sstable.getScanner(bufferPer);
            scanner.first();
            iter.addIterator(scanner);
        }
        return iter;
    }

    /**
     * Implements 'equals' in terms of intersection.
     */
    @Override
    protected boolean isEqual(SliceBuffer sb1, SliceBuffer sb2)
    {
        return scomparator.compare(sb1, sb2) == 0;
    }

    /**
     * Merges the given SliceBuffer into the merge buffer.
     *
     * If a given slice intersects a slice already in the buffer, they are resolved
     * using SliceBuffer.merge(), and the resulting slices are merged.
     *
     * FIXME: should be insertion sort.
     */
    public void reduce(SliceBuffer current)
    {
        inBufferCount++;
        ListIterator<SliceBuffer> buffiter = mergeBuff.listIterator();
        Iterator<SliceBuffer> rhsiter = Arrays.asList(current).iterator();

        SliceBuffer buffcur = null;
        SliceBuffer rhscur = null;
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
                List<SliceBuffer> resolved = SliceBuffer.merge(comparator,
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

        // update progress
        if (inBufferCount % 1000 == 0)
            updateBytesRemaining();
    }

    /**
     * Empties the merge buffer, applying garbage collection for
     * tombstones.
     *
     * TODO: To allow for arbitrarily wide rows, CompactionIterator should begin
     * returning SliceBuffers, which we can write directly back to disk. Hence, the
     * way we merge multiple Slices into a ColumnFamily is not very optimized atm.
     *
     * @return The next CompactedRow for this iterator.
     */
    @Override
    public SliceBuffer getReduced()
    {
        DecoratedKey dkey = null; 
        ColumnFamily cf = null; 
        List<SliceBuffer> toret = new LinkedList<SliceBuffer>();
        for (SliceBuffer slice : mergeBuff)
        {
            // garbage collect tombstones: may return null or an identical reference
            slice = slice.garbageCollect(major, gcBefore);
            if (slice == null)
            {
                gcCount++;
                continue;
            }
            toret.add(slice);
            outBufferCount++;
        }
        mergeBuff.clear();

        throw new RuntimeException("FIXME: Not implemented"); // FIXME
        // return toret.iterator();
    }

    public void close() throws IOException
    {
        IOException e = null;
        for (Scanner scanner : getScanners())
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
        logger.info(String.format("%s: in:%d out:%d merge-in:%d merge-out:%d gcd:%d", 
                                  this, inBufferCount, outBufferCount, inMergeCount,
                                  outMergeCount, gcCount));
        // we can only rethrow one exception, but we want to close all scanners
        if (e != null)
            throw e;
    }

    protected Iterable<Scanner> getScanners()
    {
        return ((CollatingIterator)source).getIterators();
    }

    /**
     * Based on unconsumed scanners, calculate bytesRemaining.
     */
    private void updateBytesRemaining()
    {
        long nextRemaining = 0;
        for (Scanner scanner : getScanners())
        {
            nextRemaining += scanner.getBytesRemaining();
        }
        bytesRemaining.set(nextRemaining);
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getBytesRead()
    {
        return totalBytes - bytesRemaining.get();
    }

    /**
     * Compares Slices using their key, but declares intersecting slices equal
     * so that we can resolve them.
     */
    protected static final class SliceComparator implements Comparator<SliceBuffer>
    {
        private final ColumnKey.Comparator keycomp;
        public SliceComparator(ColumnKey.Comparator keycomp)
        {
            this.keycomp = keycomp;
        }
        
        public int compare(SliceBuffer s1, SliceBuffer s2)
        {
            if (keycomp.compare(s1.begin, s2.end) < 0 &&
                keycomp.compare(s2.begin, s1.end) < 0)
                // intersection
                return 0;
            return keycomp.compare(s1.begin, s2.begin);
        }
    }

    public static final class CompactedRow
    {
        public final DecoratedKey key;
        public final DataOutputBuffer buffer;

        public CompactedRow(DecoratedKey key, DataOutputBuffer buffer)
        {
            this.key = key;
            this.buffer = buffer;
        }
    }
}
