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

import org.apache.log4j.Logger;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnKey;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

public class CompactionIterator extends AbstractIterator<SliceBuffer> implements Closeable
{
    private static Logger logger = Logger.getLogger(CompactionIterator.class);

    /**
     * Shared file buffer size for all input SSTables.
     * TODO: make configurable
     */
    public static final int TOTAL_FILE_BUFFER_BYTES = 1 << 22;

    private final int gcBefore;
    private final boolean major;

    /**
     * The comparators for all SSTables we are compacting.
     */
    private final ColumnKey.Comparator comparator;
    private final SliceComparator scomparator;
    /**
     * Scanners are kept sorted by the key of the Slice they are positioned at.
     */
    private final PriorityQueue<SSTableScanner> scanners;

    /**
     * Sorted list of Slices which are being prepared for return. As Slices are added
     * to the list, they are resolved against intersecting slices, resulting in a
     * buffer of non-intersecting slices.
     *
     * NB: This buffer is the source of the majority of memory usage for compactions.
     * Its maximum size in bytes is roughly equal to:
     * (CompactionManager.maxCompactThreshold * SSTable.TARGET_MAX_SLICE_BYTES)
     */
    private LinkedList<SliceBuffer> mergeBuff;

    /**
     * TODO: add a range-based filter like #607, but use it to seek() on the Scanners.
     */
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

        // open all scanners
        int bufferPer = TOTAL_FILE_BUFFER_BYTES / sstables.size();
        scanners = new PriorityQueue<SSTableScanner>(sstables.size(),
                                                     new ScannerComparator(comparator));
        for (SSTableReader sstable : sstables)
            scanners.add(sstable.getScanner(bufferPer));
        mergeBuff = new LinkedList<SliceBuffer>();
    }

    /**
     * Merges the given sorted, non-intersecting SliceBuffers into the merge buffer.
     *
     * If a given slice intersects a slice already in the buffer, they are resolved
     * using SliceBuffer.merge(), and the resulting slices are merged.
     */
    void mergeToBuffer(SliceBuffer... slices)
    {
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

                // FIXME:
                System.out.println("Resolved to " + resolved);

                rhsiter = Iterators.concat(resolved.iterator(), rhsiter);

                // buffcur and rhscur were consumed
                buffcur = null; rhscur = null;
            }
            else if (comp > 0)
            {
                // insert rhscur
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
        List<SSTableScanner> selected = null;
        while (!scanners.isEmpty() && scomparator.compare(scanners.peek().get(), minimum) <= 0)
        {
            if (selected == null)
                // lazily create list of scanners
                selected = new LinkedList<SSTableScanner>();
            selected.add(scanners.poll());
        }
        if (selected == null)
            // merge buffer contains minimum slice
            return true;

        // for each of the minimum scanners
        for (SSTableScanner scanner : selected)
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

        // FIXME
        System.out.println("Status");
        for (SliceBuffer buff : mergeBuff)
            System.out.println("\tsb: " + buff.key.dk + "|" + new String(buff.key.name(1)) + " -- " + buff.end.dk + "|" + (buff.end.name(1) == null ? "null" : new String(buff.end.name(1))));
        for (SSTableScanner scanner : scanners)
            System.out.println("\tsc: " + scanner.get().key.dk + "|" + new String(scanner.get().key.name(1)));

        return true;
    }

    /**
     * First, guarantees that the minimum slices for this iteration are contained in
     * the merge buffer, then pops from the head of the merge buffer, applying
     * garbage collection for tombstones.
     *
     * @return The next SliceBuffer for this iterator.
     */
    @Override
    public SliceBuffer computeNext()
    {
        while (ensureMergeBuffer())
        {
            SliceBuffer slice = mergeBuff.poll();

            // garbage collect tombstones: may return null or an identical reference
            slice = slice.garbageCollect(major, gcBefore);
            if (slice == null)
                continue;

            return slice;
        }

        // no more columns
        return endOfData();
    }

    public void close() throws IOException
    {
        IOException e = null;
        for (SSTableScanner scanner : scanners)
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
            if (keycomp.compare(s1.key, s2.end) < 0 &&
                keycomp.compare(s2.key, s1.end) < 0)
                // intersection
                return 0;
            return keycomp.compare(s1.key, s2.key);
        }
    }

    final class ScannerComparator implements Comparator<SSTableScanner>
    {
        private final ColumnKey.Comparator keycomp;
        public ScannerComparator(ColumnKey.Comparator keycomp)
        {
            this.keycomp = keycomp;
        }
        
        public int compare(SSTableScanner s1, SSTableScanner s2)
        {
            return keycomp.compare(s1.get().key, s2.get().key);
        }
    }
}
