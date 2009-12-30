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
     * List of Slices which are being prepared for return.
     *
     * NB: This buffer is the source of the majority of memory usage for compactions.
     * Its maximum size in bytes is roughly equal to:
     * (CompactionManager.maxCompactThreshold * SSTable.TARGET_MAX_SLICE_BYTES)
     *
     * The LinkedList is a natural fit for merge sort because it provides random
     * insertion performance, and allows constant time removal from the head.
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
        this.scanners = new PriorityQueue<SSTableScanner>(sstables.size());
        for (SSTableReader sstable : sstables)
            this.scanners.add(sstable.getScanner(bufferPer));
        this.mergeBuff = new LinkedList<SliceBuffer>();
    }

    /**
     * Merges the given SliceBuffer into the merge buffer.
     *
     * If two Slices intersect, they are merged on a metadata and column-by-column
     * basis, resulting in 1 or more output slices (depending on size and metadata).
     */
    void mergeToBuffer(SliceBuffer slice)
    {
        ListIterator<SliceBuffer> buffiter = mergeBuff.listIterator();
        Iterator<Column> rhsiter = rhs.iterator();

        BufferEntry buffcur = buffiter.hasNext() ? buffiter.next() : null;
        // Metadata for the slice as header
        BufferEntry rhscur = new MetadataEntry(slice.key, slice.meta);
        while (buffcur != null && rhscur != null)
        {
            // compare the heads
            int comp = buffcur.compareTo(rhscur);
            if (comp < 0)
                // merge buffer contains smaller entry
                buffcur = null;
            else if (comp == 0)
            {
                // buffcur and rhscur have equal keys and types: resolve and replace buffcur
                buffcur = resolveEntries(buffcur, rhscur);
                buffiter.set(buffcur);
                rhscur = null;
            }
            else // buffcur > rhscur
            {
                // insert smaller entry from rhs before buffcur in the merge buffer
                buffiter.set(rhscur);
                buffiter.add(buffcur);
                buffiter.previous();
                rhscur = null;
            }

            if (buffcur == null && buffiter.hasNext())
                buffcur = buffiter.next();
            if (rhscur == null && rhsiter.hasNext())
            {
                Column column = rhsiter.next();
                rhscur = new ColumnEntry(slice.key.withName(column.name()),
                                         column);
            }
        }

        // add the remainder of rhs to the end of the merge buffer
        if (rhscur != null)
            mergeBuff.add(rhscur);
        while(rhsiter.hasNext())
        {
            Column column = rhsiter.next();
            mergeBuff.add(new ColumnEntry(slice.key.withName(column.name()),
                                          column));
        }
        
        logger.trace("Added " + (rhs.size()+1) + " items to merge buffer. Contains " +
            mergeBuff.size()); // FIXME
    }

    /**
     * Resolves two BufferEntries of the same type with equal keys against one another.
     *
     * @return The resulting BufferEntry, which will never be null because parent
     * tombstone garbage collection happens as we pop entries from the merge buffer.
     */
    BufferEntry resolveEntries(BufferEntry lhs, BufferEntry rhs)
    {
        assert lhs.getClass() == rhs.getClass();

        if (lhs instanceof MetadataEntry)
        {
            Slice.Metadata lhsmeta = ((MetadataEntry)lhs).meta;
            Slice.Metadata rhsmeta = ((MetadataEntry)rhs).meta;
            // maximum deletion times at each parent level win
            return new MetadataEntry(lhs.key,
                                     Slice.Metadata.resolve(lhsmeta, rhsmeta));
        }
        // else instanceof ColumnEntry

        Column lhscol = ((ColumnEntry)lhs).column;
        Column rhscol = ((ColumnEntry)rhs).column;
        // highest priority wins
        return lhscol.comparePriority(rhscol) <= 0 ? rhs : lhs;
    }

    /**
     * Ensure that the minimum keys from all Scanners have been added to the merge buffer.
     * In the best case (since all lists are sorted) this involves a single comparison
     * of the head of the merge buffer to the head of the scanner priorityq. In the worst
     * case, it requires scanners.size() comparisons.
     *
     * @return False if the merge buffer and all Scanners are empty.
     */
    boolean ensureMergeBuffer()
    {
        // select the minimum key
        ColumnKey minimum;
        if (mergeBuff.isEmpty())
        {
            if (scanners.isEmpty())
                // the merge buffer and scanner queue are empty. we're done!
                return false;
            minimum = scanners.peek().get().key;
        }
        else
            minimum = mergeBuff.peek().key;

        // remove any scanners with keys less than or equal to the minimum
        List<SSTableScanner> selected = null;
        while (!scanners.isEmpty() && comparator.compare(scanners.peek().get().key, minimum) <= 0)
        {
            if (selected == null)
                // lazily create list of scanners
                selected = new LinkedList<SSTableScanner>();
            selected.add(scanners.poll());
        }
        if (selected == null)
            // merge buffer contains minimum key
            return true;

        // for each of the minimum slices
        for (SSTableScanner scanner : selected)
        {
            try
            {
                // merge the first slice to the merge buffer
                mergeToBuffer(scanner.get(), scanner.getBuffer());

                // skip to the next slice
                if (scanner.next())
                    // has more slices: add back to the queue to reprioritize
                    scanners.add(scanner);
                else
                    scanner.close();
            }
            catch (IOException e)
            {
                // FIXME: the iterator interface sucks for IO
                throw new IOError(e);
            }
        }
        return true;
    }

    /**
     * First, guarantees that the minimum keys for this iteration are contained in the
     * merge buffer.
     *
     * Then, while maintaining that guarantee, pops from the head of the merge
     * buffer into an output slice, while applying deletion metadata and garbage
     * collecting tombstones.
     *
     * @return The next SliceBuffer for this iterator.
     */
    @Override
    public SliceBuffer computeNext()
    {
        while (ensureMergeBuffer())
        {
            BufferEntry entry = mergeBuff.poll();
            if (entry instanceof MetadataEntry)
            {
                // metadata marks the beginning of a new slice
                MetadataEntry mentry = (MetadataEntry)entry;
                SliceBuffer oldslice = outslice;
                outslice = new SliceBuffer(mentry.key, mentry.meta);
                if (oldslice != null && !oldslice.isDeleted(major, gcBefore))
                    // return the finished slice
                    return oldslice;
                continue;
            }

            // else, ColumnEntry to add to the current slice
            ColumnEntry centry = (ColumnEntry)entry;
            if (!centry.column.isDeleted(outslice.meta, major, gcBefore))
                // add if metadata does not indicate that it should be removed
                outslice.columns.add(centry.column);

            // TODO: need to check TARGET_MAX_SLICE_BYTES here, and artificially
            // split the slice to prevent it from becoming too large
        }

        if (outslice != null && !outslice.isDeleted(major, gcBefore))
        {
            // return the final slice
            SliceBuffer oldslice = outslice;
            outslice = null;
            return oldslice;
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
     * Compares Slices using their key.
     */
    final class SliceComparator implements Comparator<Slice>
    {
        private final ColumnKey.Comparator keycomp;
        public SliceComparator(ColumnKey.Comparator keycomp)
        {
            this.keycomp = keycomp;
            throw new RuntimeException("Should compare for intersection!"); // FIXME
        }
        
        public int compare(Slice s1, Slice s2)
        {
            return keycomp.compare(s1.key, s2.key);
        }
    }
}
