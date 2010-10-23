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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ColumnObserver;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.bitidx.avro.*;
import org.apache.cassandra.utils.obs.OpenBitSet;

/**
 * Writes a binned bitmap index by observing values for the column we are indexing.
 * A bitmap index file consists of sorted bins, where each bin starts with a header segment
 * which is followed by 1 or more data segments. See the Avro schema for more information
 * on the types involved.
 *
 * A complete index is buffered in memory for every SEGMENT_SIZE_BITS rows, and then
 * flushed to a SCRATCH Component, which is added to a ScratchStack. The stack
 * handles periodically merging the scratch components, and merges all components
 * to the final destination of the index on close.
 *
 * TODO: Uses the first BitmapIndex.MAX_BINS received values to seed the bins
 *   * Better sampling/heuristics for bin selection
 *   * Non-hardcoded maximum number of bins
 */
public class BitmapIndexWriter extends BitmapIndex
{
    static final Logger logger = LoggerFactory.getLogger(BitmapIndexWriter.class);

    public static final int MERGE_FACTOR = 16;
    private static final CodecFactory DEFLATE = CodecFactory.deflateCodec(1);

    public final Observer observer;
    // id generator and stack for scratch components
    private final Component.IdGenerator gen;
    private final ScratchStack stack;
    private final int bitsPerSegment;
    // the first rowid in the current segment, and the current rowid
    private long firstRowid;
    private long curRowid;

    public BitmapIndexWriter(Descriptor desc, Component.IdGenerator gen, ColumnDefinition cdef, AbstractType nametype)
    {
        this(desc, gen, cdef, nametype, BitmapIndex.SEGMENT_SIZE_BITS);
    }

    BitmapIndexWriter(Descriptor desc, Component.IdGenerator gen, ColumnDefinition cdef, AbstractType nametype, int bitsPerSegment)
    {
        super(desc, Component.get(Component.Type.BITMAP_INDEX, gen.getNextId()), cdef);
        this.observer = new Observer(cdef.name, nametype);
        this.gen = gen;
        this.stack = new ScratchStack(desc, gen, 16);
        this.bitsPerSegment = bitsPerSegment;
        this.firstRowid = 0;
        this.curRowid = 0;
    }

    /**
     * Increment the row number we are interested in observing (initialized to 0), possibly
     * triggering flushes or merges.
     */
    public void incrementRowId() throws IOException
    {
        curRowid++;

        if (curRowid - firstRowid == bitsPerSegment)
        {
            // segments within an index should all be exactly the same size
            flush();
            firstRowid = curRowid;
        }
    }

    public void observe(ByteBuffer value)
    {
        // find the bin to the left of the value
        int idx = floor(value);
        if (idx != -1 && match(idx, value) >= 0)
        {
            // found a matching bin to our left
            ((Bin)bins.get(idx)).set(curRowid);
            return;
        }

        // value doesn't fall into an existing bin: can we create a new one?
        // (bins can only change during the first segment, since they must be identical in all segments)
        if (firstRowid == 0 && bins.size() < BitmapIndex.MAX_BINS)
        {
            // create a new single valued bin, and insert after the matched position
            Bin newbin = new Bin(value, firstRowid);
            newbin.set(curRowid);
            bins.add(idx + 1, newbin);
            return;
        }

        // choose a neighbor to expand
        Bin left = idx == -1 ? null : (Bin) bins.get(idx);
        idx++;
        Bin right = idx == bins.size() ? null : (Bin) bins.get(idx);
        if (left != null && right != null)
        {
            // expand the least dense
            if (left.bits.cardinality() < right.bits.cardinality())
                raiseMax(left, value);
            else
                lowerMin(right, value);
        }
        else if (left != null)
            raiseMax(left, value);
        else
        {
            assert right != null : "MAX_BINS must be at least 1";
            lowerMin(right, value);
        }
    }

    private void lowerMin(Bin bin, ByteBuffer value)
    {
        if (bin.max.remaining() == 0)
            // was single-valued bin
            bin.max = bin.min;
        bin.min = value;
        bin.set(curRowid);
    }

    private void raiseMax(Bin bin, ByteBuffer value)
    {
        bin.max = value;
        bin.set(curRowid);
    }

    /**
     * Clear all bits at our current rowId so that the caller can attempt error recovery.
     */
    public void reset() throws IOException
    {
        for (Binnable bin : bins)
            ((Bin)bin).clear(curRowid);
    }

    /**
     * Finish merging, and close the index.
     */
    public void close() throws IOException
    {
        logger.debug("Closing {}", this);

        // force a flush, and merge to our output component
        flush();
        stack.complete(component);
    }

    /**
     * Flush the bins to a scratch component and add it to our stack.
     */
    private void flush() throws IOException
    {
        // generate a scratch component, open for writing, and set index metadata
        Component scratch = Component.get(Component.Type.SCRATCH, gen.getNextId());
        BitmapIndexMeta meta = new BitmapIndexMeta();
        meta.cdef = cdef.deflate();
        DataFileWriter writer = new DataFileWriter(new SpecificDatumWriter<BinSegment>());
        writer.setCodec(DEFLATE);
        writer.setMeta(BitmapIndex.META_KEY, SerDeUtils.serializeWithSchema(meta).array());
        writer.create(BinSegment.SCHEMA$, new File(desc.filenameFor(scratch)));
        ReusableBinSegment segment = ReusableBinSegment.poolGet();
        try
        {
            // flush each bin as a header segment, followed by data segments
            for (Binnable binnable : bins)
            {
                Bin bin = (Bin)binnable;
                bin.serialize(segment.reuseHeader().header);
                writer.append(segment);

                // force a sync point after the header for a bin, so that we can seek directly to the data
                writer.sync();

                bin.serialize(segment.reuseData().data, curRowid - firstRowid);
                writer.append(segment);

                // reset the bin for reuse
                bin.bits.clear(0, bitsPerSegment);
                bin.setRowidOffset(curRowid);
            }
        }

        finally
        {
            ReusableBinSegment.poolReturn(segment);
            writer.close();
        }
        // and the flushed file to the stack (which might trigger merges)
        stack.add(scratch);
    }

    /**
     * An observer that informs this writer of calls.
     */
    class Observer extends ColumnObserver
    {
        Observer(ByteBuffer name, AbstractType nametype)
        {
            super(name, nametype);
        }

        @Override
        public void observe(ByteBuffer value)
        {
            BitmapIndexWriter.this.observe(value);
        }
    }

    /**
     * Extends Binnable to add a bitmap describing rows matching the bin.
     */
    static final class Bin extends Binnable
    {
        private final OpenBitSet bits;
        private long rowid;
        public Bin(ByteBuffer min, long rowid)
        {
            super(min);
            this.bits = new OpenBitSet();
            this.rowid = rowid;
        }

        /** Set the absolute rowid inside the relative storage of the bin. */
        public void set(long set)
        {
            bits.set(set - rowid);
        }

        /** Clear the absolute rowid inside the relative storage of the bin. */
        public void clear(long clear)
        {
            bits.clear(clear - rowid);
        }

        /** Set the relative offset of this bin. */
        public void setRowidOffset(long rowid)
        {
            this.rowid = rowid;
        }

        public void serialize(BinHeader header)
        {
            header.min = min;
            header.max = max;
            header.cardinality = bits.cardinality();
        }

        public void serialize(BinData data, long numrows)
        {
            data.rowid = rowid;
            data.numrows = numrows;
            // serialize bits: we don't use Avro longs directly, since they're boxed
            data.bits = SerDeUtils.reuse(data.bits, bits.getNumWords() << 3);
            data.bits.asLongBuffer().put(bits.getBits(), 0, bits.getNumWords());
        }
    }
}
