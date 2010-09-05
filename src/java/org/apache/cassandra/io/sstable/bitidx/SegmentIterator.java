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
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.cassandra.io.sstable.bitidx.avro.*;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * Iterates over segments for a particular bin of this index, within rowid bounds.
 * Objects returned by this iterator are only valid between calls to next().
 * FIXME: Should only deserialize the interesting words in a bin.
 */
final class SegmentIterator implements CloseableIterator<OpenSegment>
{
    static final Logger logger = LoggerFactory.getLogger(SegmentIterator.class);

    private final DataFileReader<BinSegment> reader;
    private final long endRowid;

    private boolean isExhausted;
    // true if the currently held segment has not yet been deserialized/returned
    private boolean hasBinSegment;
    private ReusableBinSegment binSegment;
    private OpenSegment segment;

    private SegmentIterator(DataFileReader reader, long endRowid)
    {
        this.reader = reader;
        this.endRowid = endRowid;
        this.isExhausted = false;
        this.hasBinSegment = false;
        this.binSegment = ReusableBinSegment.poolGet();
        // TODO: pool for OpenSegments
        this.segment = new OpenSegment(BitmapIndex.SEGMENT_SIZE_BITS);
    }

    /**
     * File to iterate, an Avro sync point to start from, and a rowid to bound the search (although
     * this iterator always stops when it reaches the end of a bin).
     */
    static SegmentIterator open(File file, long startSync, long endRowid) throws IOError
    {
        SpecificDatumReader sdr = new SpecificDatumReader<BinSegment>();
        try
        {
            DataFileReader<BinSegment> reader = new DataFileReader<BinSegment>(file, sdr);
            reader.seek(startSync);
            return new SegmentIterator(reader, endRowid);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public boolean hasNext()
    {
        if (hasBinSegment)
            return true;
        if (isExhausted || !reader.hasNext())
            // no more interesting rows, or reached eof
            return false;
        try
        {
            // more segments available
            binSegment.readFrom(reader);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        if (binSegment.data == null || binSegment.data.rowid > endRowid)
        {
            // reached a header segment for the next bin, or no more interesting rows
            isExhausted = true;
            return false;
        }
        hasBinSegment = true;
        return true;
    }

    public OpenSegment next()
    {
        if (!hasNext()) throw new NoSuchElementException();
        // deserialize the current binary segment, and mark it consumed
        segment.deserialize(binSegment.data);
        hasBinSegment = false;
        return segment;
    }

    public void close() throws IOException
    {
        ReusableBinSegment.poolReturn(binSegment);
        binSegment = null;
        reader.close();
    }

    public void remove() { throw new UnsupportedOperationException(); }

    static final CloseableIterator<OpenSegment> EMPTY = new CloseableIterator<OpenSegment>()
    {
        public boolean hasNext() { return false; }
        public OpenSegment next() { throw new NoSuchElementException(); }
        public void close() {}
        public void remove() { throw new UnsupportedOperationException(); }
    };
}
