/**
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
 */

package org.apache.cassandra.io.sstable.bitidx;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.bitidx.avro.*;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.apache.cassandra.utils.Pair;

// yuck
import org.apache.cassandra.thrift.IndexOperator;

/**
 * Holds a summary of a BitmapIndex file on disk, and exposes query operations.
 */
public class BitmapIndexReader extends BitmapIndex
{
    static final Logger logger = LoggerFactory.getLogger(BitmapIndexReader.class);

    protected final File file;

    public BitmapIndexReader(File file, Descriptor desc, Component component, ColumnDefinition cdef)
    {
        super(desc, component, cdef);
        this.file = file;
    }

    /** @return The ColumnDefinition for the given BitmapIndexReader component. */
    public static ColumnDefinition metadata(Descriptor desc, Component component) throws IOException
    {
        BitmapIndexMeta meta = new BitmapIndexMeta();
        File file = new File(desc.filenameFor(component));
        DataFileReader<BinSegment> reader = new DataFileReader<BinSegment>(file, new SpecificDatumReader<BinSegment>());
        try
        {
            SerDeUtils.deserializeWithSchema(ByteBuffer.wrap(reader.getMeta(BitmapIndex.META_KEY)), meta);
        }
        finally
        {
            reader.close();
        }
        return ColumnDefinition.inflate(meta.cdef);
    }

    public static BitmapIndexReader open(Descriptor desc, Component component) throws IOException
    {
        BitmapIndexReader bmir;
        File file = new File(desc.filenameFor(component));
        DataFileReader<BinSegment> reader = new DataFileReader<BinSegment>(file, new SpecificDatumReader<BinSegment>());
        ReusableBinSegment segment = ReusableBinSegment.poolGet();
        try
        {
            // retrieve the index metadata
            BitmapIndexMeta meta = new BitmapIndexMeta();
            SerDeUtils.deserializeWithSchema(ByteBuffer.wrap(reader.getMeta(BitmapIndex.META_KEY)), meta);
            bmir = new BitmapIndexReader(file, desc, component, ColumnDefinition.inflate(meta.cdef));

            // summarize the segments in the file
            boolean syncNext = false;
            long previousSync = reader.previousSync();
            long rowId = 0;
            BinSummary bin = null;
            while (reader.hasNext())
            {
                segment.readFrom(reader);
                if (segment.header != null)
                {
                    // beginning of a new bin
                    bin = new BinSummary(ByteBuffer.wrap(SerDeUtils.copy(segment.header.min)),
                                         ByteBuffer.wrap(SerDeUtils.copy(segment.header.max)),
                                         segment.header.cardinality);
                    bmir.bins.add(bin);
                    rowId = 0;
                }
                else
                {
                    // data for the current bin
                    if (syncNext)
                        // this segment is externally accessible: store an offset
                        bin.segments.add(new BinSummary.Offset(rowId, previousSync));
                    rowId += segment.data.numrows;
                }

                // if this read caused the sync point to change, the next record falls on a sync point
                if (previousSync != reader.previousSync())
                {
                    syncNext = true;
                    previousSync = reader.previousSync();
                }
                else
                    syncNext = false;
            }
            bmir.bins.trimToSize();
        }
        finally
        {
            ReusableBinSegment.poolReturn(segment);
            reader.close();
        }
        return bmir;
    }

    /**
     * @return The number of rows matched by the given value.
     */
    public long cardinality(IndexOperator op, ByteBuffer value)
    {
        Pair<Integer,Integer> binRange = findBins(op, value);
        if (binRange.left == -1 || binRange.right == -1)
           return 0;
        long selected = 0;
        for (int i = binRange.left; i <= binRange.right; i++)
            selected += ((BinSummary)bins.get(i)).cardinality;
        return selected;
    }

    private Pair<Integer,Integer> findBins(IndexOperator op, ByteBuffer value)
    {
        int minbin = -1;
        int maxbin = -1;
        switch (op)
        {
            case EQ:
                minbin = floor(value);
                if (minbin != -1 && match(minbin, value) >= 0)
                    // found exactly one bin
                    maxbin = minbin;
                break;
            case GTE:
                minbin = floor(value);
                if (minbin == -1 || match(minbin, value) < 0)
                    // floored bin doesn't exist/match, exclude it
                    minbin += 1;
                maxbin = bins.size() - 1;
                break;
            case LTE:
                // floored bin must contain our bound
                minbin = 0;
                maxbin = floor(value);
                break;
            case GT:
                minbin = floor(value);
                if (minbin == -1)
                    // round up to first bin
                    minbin += 1;
                else
                {
                    int match = match(minbin, value);
                    // exclude floored bin if we matched the max, or didn't match
                    if (match == 2 || match < 0)
                        minbin += 1;
                }
                maxbin = bins.size() - 1;
                break;
            case LT:
                minbin = 0;
                maxbin = floor(value);
                // include floored bin unless we matched the min
                if (maxbin != -1 && match(maxbin, value) == 0)
                    maxbin -= 1;
                break;
            default: throw new UnsupportedOperationException();
        }
        return new Pair<Integer,Integer>(minbin, maxbin);
    }

    /**
     * @return An iterator over OpenSegments for the given value and operator which intersect the given rowid range.
     */
    public CloseableIterator<OpenSegment> iterator(IndexOperator op, ByteBuffer value, Pair<Long,Long> rowidrange)
    {
        Pair<Integer,Integer> binRange = findBins(op, value);
        if (logger.isDebugEnabled())
            logger.debug("For operator " + op + " on " + cdef.validator.getString(value) + " in rows " + rowidrange + ": bins " + binRange + " of " + this);

        // open iterators for covering syncs/rowids within the bins
        if (binRange.left == -1 || binRange.right == -1)
            // no bins? no matches.
            return SegmentIterator.EMPTY;
        if (binRange.left == binRange.right)
            // exactly one bin
            return openBin(binRange.left, rowidrange);

        // else, a range of bins is involved: open iterators for each, and join them
        ArrayList<CloseableIterator<OpenSegment>> sources = new ArrayList<CloseableIterator<OpenSegment>>(binRange.right - binRange.left);
        for (int i = binRange.left; i <= binRange.right; i++)
            sources.add(openBin(i, rowidrange));
        return new JoiningIterator(JoiningIterator.Operator.OR, sources);
    }

    private SegmentIterator openBin(int idx, Pair<Long, Long> rowidrange)
    {
        long syncPoint = ((BinSummary)bins.get(idx)).syncPoint(rowidrange.left);
        return SegmentIterator.open(file, syncPoint, rowidrange.right);
    }

    static final class BinSummary extends Binnable
    {
        public final ArrayList<Offset> segments = new ArrayList<Offset>();
        public final long cardinality;

        public BinSummary(ByteBuffer min, ByteBuffer max, long cardinality)
        {
            super(min, max);
            this.cardinality = cardinality;
        }
        
        /**
         * @return A sync point to scan from to find the given rowid in this bucket.
         */
        public long syncPoint(long rowid)
        {
            // min
            int idx = Collections.binarySearch(segments, new Offset(rowid, -1));
            if (idx < 0)
                // round down: we always can, since the writer syncs at 0 for all bins
                idx = -(2 + idx);
            assert idx >= 0 : "Out of bounds in " + segments;
            return segments.get(idx).offset;
        }

        static final class Offset implements Comparable<Offset>
        {
            public final long rowid;
            public final long offset;
            public Offset(long rowid, long offset)
            {
                this.rowid = rowid;
                this.offset = offset;
            }

            public int compareTo(Offset that)
            {
                if (this.rowid < that.rowid) return -1;
                if (this.rowid > that.rowid) return 1;
                return 0;
            }

            public String toString()
            {
                return "(" + rowid + "," + offset + ")";
            }
        }
    }
}
