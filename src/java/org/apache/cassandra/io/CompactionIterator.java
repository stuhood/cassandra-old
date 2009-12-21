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
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnKey;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.AbstractIterator;

public class CompactionIterator extends AbstractIterator<CompactionColumn> implements Closeable
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
     * The comparator and columnDepth, which should be the same for all SSTables we
     * are compacting.
     */
    private final ColumnKey.Comparator comparator;
    private final int columnDepth;
    /**
     * Scanners are kept sorted by the key of the Slice they are positioned at.
     */
    private final PriorityQueue<SSTableScanner> scanners;

    /**
     * In each iteration, the head of this buffer is compared to the head of every
     * Slice, and if any Slices have heads less than the head of the buffer, they are
     * merged/resolved into the buffer. The merge/resolve process takes creation,
     * deletion and gc times into account, and results in the head of the buffer
     * being the smallest CompactionColumn in any of the input SSTables.
     */
    private Deque<CompactionColumn> mergeBuff;

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
        this.comparator = head.getComparator();
        this.columnDepth = head.getColumnDepth();

        // open all scanners
        int bufferPer = TOTAL_FILE_BUFFER_BYTES / sstables.size();
        this.scanners = new PriorityQueue<SSTableScanner>(sstables.size());
        for (SSTableReader sstable : sstables)
            this.scanners.add(sstable.getScanner(bufferPer));
        this.mergeBuff = new ArrayDeque<CompactionColumn>();
    }

    /**
     * Remove and return scanners from the priorityq whose keys are less than or
     * equal to the head of the merge buffer. If the merge buffer is empty, returns
     * the scanners containing the minimum slices.
     *
     * @return A possibly empty list of matching Scanners.
     */
    public List<SSTableScanner> removeMinimumScanners()
    {
        ColumnKey minimum;
        if (mergeBuff.isEmpty())
        {
            if (scanners.isEmpty())
                // the merge buffer and scanner queue are empty. we're done!
                return Collections.<SSTableScanner>emptyList();;
            minimum = scanners.peek().get().currentKey;
        }
        else
        {
            minimum = mergeBuff.peek().key;
        }

        // select any scanners with keys equal to the minimum
        List<SSTableScanner> selected = new LinkedList<SSTableScanner>();
        while (!scanners.isEmpty() &&
               comparator.compare(minimum, scanners.peek().get().currentKey) == 0)
        {
            selected.add(scanners.poll());
        }
        return selected;
    }

    @Override
    public CompactionColumn computeNext()
    {
        // for each of the minimum slices
        for (SSTableScanner scanner : removeMinimumScanners())
        {
            // merge the slice to the merge buffer
            merge(scanner.get(), scanner.getColumns());

            // skip to the next slice
            if (scanner.next())
                // has more slices: reprioritize
                scanners.add(scanner);
        }

        if (mergeBuff.isEmpty())
            // no more columns
            return endOfData();

        return mergeBuff.poll();
        /*


        assert rows.size() > 0;
        DataOutputBuffer buffer = new DataOutputBuffer();
        DecoratedKey key = rows.get(0).getKey();

        ColumnFamily cf = null;
        try
        {
            if (rows.size() > 1 || major)
            {
                for (Pair<DecoratedKey,ColumnFamily> row : rows)
                {
                    ColumnFamily thisCF;
                    try
                    {
                        thisCF = row.getColumnFamily();
                    }
                    catch (IOException e)
                    {
                        logger.error("Skipping row " + key + " in " + row.getPath(), e);
                        continue;
                    }
                    if (cf == null)
                    {
                        cf = thisCF;
                    }
                    else
                    {
                        cf.addAll(thisCF);
                    }
                }
                ColumnFamily cfPurged = major ? ColumnFamilyStore.removeDeleted(cf, gcBefore) : cf;
                if (cfPurged == null)
                    return null;
                ColumnFamily.serializer().serializeWithIndexes(cfPurged, buffer);
                cf = cfPurged;
            }
            else
            {
                assert rows.size() == 1;
                try
                {
                    rows.get(0).echoData(buffer);
                    // FIXME: see the explanation attached to CompactionRow
                    cf = rows.get(0).getColumnFamily();
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        }
        finally
        {
            rows.clear();
        }
        return new CompactedRow(key, buffer, cf);

        */
    }

    public void close() throws IOException
    {
        for (SSTableScanner scanner : scanners)
            scanner.close();
    }

    /**
     * Class representing a column after compaction.
     */
    public static final class CompactionColumn
    {
        public final ColumnKey key;
        public final Slice.Metadata meta;
        public final Column column;

        public CompactionColumn(ColumnKey key, Slice.Metadata meta, Column column)
        {                   
            this.key = key;
            this.meta = meta;
            this.column = column;
        }

        /**
         * Digest the key, metadata and content of this column.
         */
        public byte[] digest()
        {
            throw new RuntimeException("Not implemented."); // FIXME
        }
    }
}
