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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.avro.file.DataFileReader;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.bitidx.avro.*;
import org.apache.cassandra.utils.obs.OpenBitSet;

/**
 * Extends BinSegment to preserve the content of unions during object reuse:
 * if references are not stored to each of the possible values for a union, the
 * values that do not match will be gc'd for every reuse.
 */
final class ReusableBinSegment extends BinSegment
{
    /**
     * The maximum number of ReusableBinSegments that will be pooled. Steady state memory
     * usage should be around SEGMENT_SIZE_BYTES * CACHED_REUSABLE_SEGMENTS for all
     * indexes in the process.
     */
    private final static int POOLED_SEGMENT_COUNT = 16 * DatabaseDescriptor.getConcurrentReaders();

    // a capped pool of ReusableBinSegments: accessed via segmentPool(Get|Return)
    private final static ArrayBlockingQueue<ReusableBinSegment> POOLED_SEGMENTS = new ArrayBlockingQueue<ReusableBinSegment>(POOLED_SEGMENT_COUNT);

    static final ReusableBinSegment poolGet()
    {
        ReusableBinSegment segment = POOLED_SEGMENTS.poll();
        if (segment != null)
            return segment;
        return new ReusableBinSegment();
    }

    static final void poolReturn(ReusableBinSegment segment)
    {
        // NB: if the pool is full, we want the object to be dropped
        POOLED_SEGMENTS.offer(segment);
    }

    /*************************************************************************/

    private final BinHeader reusable_header = new BinHeader();
    private final BinData reusable_data = new BinData();
    {
        reusable_header.min = ByteBuffer.allocate(128);
        reusable_header.max = ByteBuffer.allocate(128);
        reusable_data.bits = ByteBuffer.allocate(BitmapIndex.SEGMENT_SIZE_BYTES);
    }

    private ReusableBinSegment reuse(BinHeader h, BinData d)
    {
        header = h;
        data = d;
        return this;
    }

    /**
     * We make an assumption about the implementation of reader.read(reuse), so
     * this method handles asserting that it is true.
     */
    public void readFrom(DataFileReader<BinSegment> reader) throws IOException
    {
        BinSegment segment = reader.next(reuse(reusable_header, reusable_data));
        assert segment == this : "Avro schema does not support object reuse.";
    }

    public ReusableBinSegment reuseHeader()
    {
        return reuse(reusable_header, null);
    }

    public ReusableBinSegment reuseData()
    {
        return reuse(null, reusable_data);
    }
}
