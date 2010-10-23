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
 *
 * TODO: Current implementation is very naive. Problems, in order of importance:
 * 1. Buffers entire index into memory
 *   * Should flush to temporary files and perform compactions
 * 2. Uses the first BitmapIndex.MAX_BINS received values to seed the bins
 *   * Better sampling/heuristics for bin selection
 *   * Non-hardcoded maximum number of bins
 */
public class BitmapIndexWriter extends BitmapIndex
{
    static final Logger logger = LoggerFactory.getLogger(BitmapIndexWriter.class);

    private static final CodecFactory DEFLATE = CodecFactory.deflateCodec(1);

    public final Observer observer;
    // id generator for scratch components we'll create during flush and merge stages
    private final Component.IdGenerator gen;
    // the first rowid in the current segment, and the current rowid
    private long firstRowid;
    private long curRowid;

    public BitmapIndexWriter(Descriptor desc, Component.IdGenerator gen, ColumnDefinition cdef, AbstractType nametype)
    {
        super(desc, new Component(Component.Type.BITMAP_INDEX, gen.getNextId()), cdef);
        this.observer = new Observer(cdef.name, nametype);
        this.gen = gen;
        this.firstRowid = 0;
        this.curRowid = 0;
    }

    /**
     * Increment the row number we are interested in observing (initialized to 0).
     */
    public void incrementRowId()
    {
        curRowid++;
        assert curRowid < BitmapIndex.SEGMENT_SIZE_BITS : "FIXME: Implement flushing.";
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
     * Flush and close the index.
     */
    public void close() throws IOException
    {
        logger.debug("Closing {}", this);

        // open for writing, and set index metadata
        BitmapIndexMeta meta = new BitmapIndexMeta();
        meta.cdef = cdef.deflate();
        DataFileWriter writer = new DataFileWriter(new SpecificDatumWriter<BinSegment>());
        writer.setCodec(DEFLATE);
        writer.setMeta(BitmapIndex.META_KEY, SerDeUtils.serializeWithSchema(meta).array());
        writer.create(BinSegment.SCHEMA$, new File(desc.filenameFor(component)));
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
                bin.bits.clear(0, BitmapIndex.SEGMENT_SIZE_BITS);
                bin.setRowidOffset(curRowid);
            }
        }
        finally
        {
            ReusableBinSegment.poolReturn(segment);
            writer.close();
        }
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
