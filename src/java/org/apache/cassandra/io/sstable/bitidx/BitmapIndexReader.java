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
            SerDeUtils.deserializeWithSchema(reader.getMeta(BitmapIndex.META_KEY), meta);
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
                    bin = new BinSummary(SerDeUtils.copy(segment.header.min),
                                         SerDeUtils.copy(segment.header.max),
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
        }
        finally
        {
            ReusableBinSegment.poolReturn(segment);
            reader.close();
        }
        return bmir;
    }

    /**
     * @return An iterator over OpenSegments for the given value and operator which intersect the given rowid range.
     */
    public CloseableIterator<OpenSegment> iterator(IndexOperator op, byte[] value, Pair<Long,Long> rowidrange)
    {
        // FIXME: supporting other IndexOperators will require a Compound BMI
        if (op != IndexOperator.EQ)
            throw new UnsupportedOperationException();
        
        // find matching bins
        int idx = floor(value);
        BinSummary bin = idx == -1 ? null : (BinSummary) bins.get(idx);
        if (bin == null || !bin.matches(cdef.validator, value))
            return SegmentIterator.EMPTY;
        
        // open an iterator for covering sync markers within the bin for the rowids
        return SegmentIterator.open(file, bin.syncPoint(rowidrange.left), rowidrange.right);
    }

    static final class BinSummary extends Binnable
    {
        public final ArrayList<Offset> segments = new ArrayList<Offset>();

        public BinSummary(byte[] min, byte[] max)
        {
            super(min, max);
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
