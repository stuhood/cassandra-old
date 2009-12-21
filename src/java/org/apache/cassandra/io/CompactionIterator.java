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
     * Queue of Metadata and Column entries. Metadata entries apply to all columns up
     * to the next Metadata entry. See BufferEntry.
     *
     * NB: This buffer is the source of the majority of memory usage for compactions.
     * Its maximum size in bytes is roughly equal to:
     * (CompactionManager.maxCompactThreshold * SSTableWriter.TARGET_MAX_SLICE_BYTES)
     */
    private PriorityQueue<BufferEntry> mergeBuff;
    /**
     * Metadata for the current output slice. Whenever a Metadata entry reaches the
     * front of the merge buffer, it is stored here to apply and resolve with columns
     * that follow.
     */
    private Slice.Metadata outmeta;

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
     * the scanners starting with the minimum keys.
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

        // select any scanners with keys less than or equal to the minimum
        List<SSTableScanner> selected = new LinkedList<SSTableScanner>();
        while (!scanners.isEmpty() &&
               comparator.compare(scanners.peek().get().currentKey, minimum) <= 0)
        {
            selected.add(scanners.poll());
        }
        return selected;
    }

    /**
     * Merges the given slice into the merge buffer.
     * FIXME: We're using a PriorityQueue, so this isn't actually merge-sort:
     *        implement it here for maximum efficiency.
     */
    public void merge(Slice slice, List<Column> columns)
    {
        // add a Metadata entry for the slice header
        mergeBuff.add(new MetadataEntry(slice.currentKey, slice.meta));
        // and Column entries for each column
        for (Column column : columns)
            mergeBuff.add(new ColumnEntry(slice.currentKey.withName(column.name()),
                                          column));
            
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

        // find the first column in the merge buffer
        // FIXME: entries with equal keys need to be resolved here:
        //        Metadata or otherwise
        while (!mergeBuff.isEmpty())
        {
            BufferEntry entry = mergeBuff.poll();
            if (entry instanceof MetadataEntry)
            {
                // popped a Metadata object: apply to columns that follow
                outmeta = ((MetadataEntry)entry).meta;
                continue;
            }
            
            ColumnEntry colentry = (ColumnEntry)entry;
            assert outmeta != null;
            return new CompactionColumn(colentry.key, outmeta, colentry.column);
        }

        // no more columns
        return endOfData();

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

    /**
     * Represents a tuple of (ColumnKey, (Metadata or Column)). Metadata entries in
     * the buffer play a similar role to the one they play on disk: they apply
     * metadata to all columns up to the next Metadata entry.
     */
    abstract class BufferEntry implements Comparable<BufferEntry>
    {
        public final ColumnKey key;
        protected BufferEntry(ColumnKey key)
        {
            this.key = key;
        }
            
        public int compareTo(BufferEntry that)
        {
            return comparator.compare(this.key, that.key);
        }
    }

    class MetadataEntry extends BufferEntry
    {
        public final Slice.Metadata meta;
        public MetadataEntry(ColumnKey key, Slice.Metadata meta)
        {
            super(key);
            this.meta = meta;
        }
    }

    class ColumnEntry extends BufferEntry
    {
        public final Column column;
        public ColumnEntry(ColumnKey key, Column column)
        {
            super(key);
            this.column = column;
        }
    }
}
