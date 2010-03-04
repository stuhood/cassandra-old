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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.Scanner;
import org.apache.cassandra.io.util.DataOutputBuffer;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

public class CompactionIterator extends AbstractIterator<CompactionIterator.CompactedRow> implements Closeable
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
     * Scanners are kept sorted by the key of the Slice they are positioned at.
     */
    private final PriorityQueue<Scanner> scanners;

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
        super();
        assert !sstables.isEmpty();

        this.gcBefore = gcBefore;
        this.major = major;

        // fields shared for all sstables
        SSTableReader head = sstables.iterator().next();
        comparator = head.getComparator();
        scomparator = new SliceComparator(comparator);
        emptycf = head.makeColumnFamily();
        depth = head.getColumnDepth();

        // open all scanners
        scanners = new PriorityQueue<Scanner>(sstables.size(),
                                              new ScannerComparator(comparator));
        scanners.addAll(getScanners(sstables));

        updateBytesRemaining();
        totalBytes = bytesRemaining.get();

        mergeBuff = new LinkedList<SliceBuffer>();
    }

    protected Collection<Scanner> getScanners(Collection<SSTableReader> sstables) throws IOException
    {
        List<Scanner> ret = new ArrayList<Scanner>(sstables.size());
        int bufferPer = TOTAL_FILE_BUFFER_BYTES / sstables.size();
        for (SSTableReader sstable : sstables)
        {
            SSTableScanner scanner = sstable.getScanner(bufferPer);
            scanner.first();
            ret.add(scanner);
        }
        return ret;
    }

    /**
     * Merges the given sorted, non-intersecting SliceBuffers into the merge buffer.
     *
     * If a given slice intersects a slice already in the buffer, they are resolved
     * using SliceBuffer.merge(), and the resulting slices are merged.
     */
    void mergeToBuffer(SliceBuffer... slices)
    {
        inBufferCount += slices.length;
        ListIterator<SliceBuffer> buffiter = mergeBuff.listIterator();
        Iterator<SliceBuffer> rhsiter = Arrays.asList(slices).iterator();

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
    }

    /**
     * Ensure that the minimum slices from all Scanners have been added to the merge
     * buffer.
     *
     * @return False if the merge buffer and all Scanners are empty.
     */
    boolean ensureMergeBuffer()
    {
        // select the minimum slice
        Slice minimum;
        if (mergeBuff.isEmpty())
        {
            if (scanners.isEmpty())
                // the merge buffer and scanner queue are empty. we're done!
                return false;
            minimum = scanners.peek().get();
        }
        else
            minimum = mergeBuff.peek();

        // remove any scanners with slices less than or intersecting the minimum
        List<Scanner> selected = null;
        while (!scanners.isEmpty() && scomparator.compare(scanners.peek().get(), minimum) <= 0)
        {
            if (selected == null)
                // lazily create list of scanners
                selected = new LinkedList<Scanner>();
            selected.add(scanners.poll());
        }
        if (selected == null)
            // merge buffer contains minimum slice
            return true;

        // for each of the minimum scanners
        for (Scanner scanner : selected)
        {
            try
            {
                // merge the first slice to the merge buffer
                mergeToBuffer(scanner.getBuffer());

                // skip to the next slice
                if (scanner.next())
                    // has more slices: add back to the queue to reprioritize
                    scanners.add(scanner);
                else
                    // consumed: close early
                    scanner.close();
            }
            catch (IOException e)
            {
                // the iterator interface sucks for IO
                throw new IOError(e);
            }
        }
        if (inBufferCount % 1000 == 0)
            updateBytesRemaining();
        return true;
    }

    /**
     * First, guarantees that the minimum slices for this iteration are contained in
     * the merge buffer, then pops from the head of the merge buffer, applying
     * garbage collection for tombstones.
     *
     * TODO: To allow for arbitrarily wide rows, CompactionIterator should begin
     * returning SliceBuffers, which we can write directly back to disk. Hence, the
     * way we merge multiple Slices into a ColumnFamily is not very optimized atm.
     *
     * @return The next CompactedRow for this iterator.
     */
    @Override
    public CompactedRow computeNext()
    {
        DecoratedKey dkey = null; 
        ColumnFamily cf = null; 
        List<SliceBuffer> cfslices = null;
        while (ensureMergeBuffer())
        {
            // check that the slice is part of the current cf
            if (dkey != null && dkey.compareTo(mergeBuff.peek().begin.dk) != 0)
                // new slice is part of the next cf
                break;
            SliceBuffer slice = mergeBuff.poll();

            // garbage collect tombstones: may return null or an identical reference
            slice = slice.garbageCollect(major, gcBefore);
            if (slice == null)
            {
                gcCount++;
                continue;
            }

            // add slice columns
            outBufferCount++;
            if (cf == null)
            {
                dkey = slice.begin.dk;
                cf = emptycf.cloneMeShallow();
                cfslices = new ArrayList<SliceBuffer>();
                cf.delete(slice.meta.localDeletionTime, slice.meta.markedForDeleteAt);
            }
            cfslices.add(slice);
        }

        if (cf == null)
            return endOfData();

        // populate the cf with its list of slices
        if (depth == 1)
            populateStandardCF(cf, cfslices);
        else
            populateSuperCF(cf, cfslices);

        // serialize to the compacted row
        DataOutputBuffer dao = new DataOutputBuffer();
        ColumnFamily.serializer().serializeWithIndexes(cf, dao);
        return new CompactedRow(dkey, dao);
    }

    private void populateStandardCF(ColumnFamily cf, List<SliceBuffer> cfslices)
    {
        for (SliceBuffer slice : cfslices)
            for (Column col : slice.realized())
                cf.addColumn(col);
    }

    private void populateSuperCF(ColumnFamily cf, List<SliceBuffer> cfslices)
    {
        for (SliceBuffer slice : cfslices)
        {
            byte[] scname = slice.begin.name(1);
            SuperColumn sc = (SuperColumn)cf.getColumn(scname);
            if (sc == null)
                cf.addColumn(sc = new SuperColumn(scname, comparator.typeAt(2)));
            for (Column col : slice.realized())
                sc.addColumn(col);
        }
    }

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
        logger.info(String.format("%s: in:%d out:%d merge-in:%d merge-out:%d gcd:%d", 
                                  this, inBufferCount, outBufferCount, inMergeCount,
                                  outMergeCount, gcCount));
        // we can only rethrow one exception, but we want to close all scanners
        if (e != null)
            throw e;
    }

    /**
     * Based on unconsumed scanners, calculate bytesRemaining.
     */
    private void updateBytesRemaining()
    {
        long nextRemaining = 0;
        for (Scanner scanner : scanners)
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
    final class SliceComparator implements Comparator<Slice>
    {
        private final ColumnKey.Comparator keycomp;
        public SliceComparator(ColumnKey.Comparator keycomp)
        {
            this.keycomp = keycomp;
        }
        
        public int compare(Slice s1, Slice s2)
        {
            if (keycomp.compare(s1.begin, s2.end) < 0 &&
                keycomp.compare(s2.begin, s1.end) < 0)
                // intersection
                return 0;
            return keycomp.compare(s1.begin, s2.begin);
        }
    }

    final class ScannerComparator implements Comparator<Scanner>
    {
        private final ColumnKey.Comparator keycomp;
        public ScannerComparator(ColumnKey.Comparator keycomp)
        {
            this.keycomp = keycomp;
        }
        
        public int compare(Scanner s1, Scanner s2)
        {
            return keycomp.compare(s1.get().begin, s2.get().begin);
        }
    }

    public static class CompactedRow
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
