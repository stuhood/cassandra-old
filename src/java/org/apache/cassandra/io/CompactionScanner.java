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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Scanner;
import org.apache.cassandra.MergingScanner;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.FilteredScanner;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;

import com.google.common.collect.Iterators;

public class CompactionScanner extends MergingScanner
{
    private static Logger logger = LoggerFactory.getLogger(CompactionScanner.class);

    // shared file buffer size for all input SSTables.
    public static final int TOTAL_FILE_BUFFER_BYTES = 1 << 22;

    // total bytes in all input scanners
    private final long totalBytes;
    // bytes remaining in input scanners
    private final AtomicLong bytesRemaining = new AtomicLong();

    public CompactionScanner(Collection<SSTableReader> sstables, QueryFilter filter, ColumnKey.Comparator comparator) throws IOException
    {
        super(getScanners(sstables, filter), comparator);

        // determine total length
        totalBytes = getBytesRemaining();
        bytesRemaining.set(totalBytes);
    }

    private static List<Scanner> getScanners(Collection<SSTableReader> sstables, QueryFilter filter) throws IOException
    {
        List<Scanner> scanners = new ArrayList<Scanner>();
        int bufferPer = TOTAL_FILE_BUFFER_BYTES / Math.max(1, sstables.size());
        for (SSTableReader sstable : sstables)
        {
            Scanner scanner = filter == null ?
                sstable.getScanner(bufferPer) : new FilteredScanner(sstable.getScanner(bufferPer), filter);
            scanners.add(scanner);
        }
        return scanners;
    }

    @Override
    public ASlice transformed()
    {
        // periodically update progress
        if (outBufferCount % 10000 == 0)
            bytesRemaining.set(getBytesRemaining());

        return super.transformed();
    }

    @Override
    public void close() throws IOException
    {
        logger.info(String.format("%s: in:%d out:%d merge-in:%d merge-out:%d", this, inBufferCount, outBufferCount, inMergeCount, outMergeCount));
        super.close();
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getBytesRead()
    {
        return totalBytes - bytesRemaining.get();
    }

    public static final class CompactedRow
    {
        public final DecoratedKey key;
        public final DataOutputBuffer buffer;

        public CompactedRow(DecoratedKey key, DataOutputBuffer buffer)
        {
            this.key = key;
            this.buffer = buffer;
        }
    }
}
