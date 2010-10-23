/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.io.sstable.SSTableUtils;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.bitidx.avro.*;
import org.apache.cassandra.utils.FBUtilities;

public class BitmapIndexTest extends CleanupHelper
{
    public static final String KSNAME = "Keyspace1";
    public static final String INDEXEDCFNAME = "Indexed3";
    public static final String INDEXNAME = "state";
    
    public static ColumnDefinition CDEF;
    public static AbstractType NAMETYPE;

    @BeforeClass
    public static void prepareClass()
    {
        CFMetaData cfm = DatabaseDescriptor.getCFMetaData(KSNAME, INDEXEDCFNAME);
        CDEF = cfm.getColumn_metadata().get(Util.bytes(INDEXNAME));
        NAMETYPE = cfm.comparator;
    }

    /** Write an index with very small segments to exercise the stack that merges during writing. */
    @Test
    public void testScratchStack() throws IOException
    {
        int SMALL_SEGMENT_SIZE = 100;
        int CARDINALITY = 10;
        // enough rows to trigger a few full merges
        int TOTAL_ROWS = SMALL_SEGMENT_SIZE * BitmapIndexWriter.MERGE_FACTOR;
        Descriptor desc = Descriptor.fromFilename(SSTableUtils.tempSSTableFile(KSNAME, INDEXEDCFNAME).toString());
        
        BitmapIndexWriter w = new BitmapIndexWriter(desc, new Component.IdGenerator(), CDEF, NAMETYPE, SMALL_SEGMENT_SIZE);
        for (int i = 0; i < TOTAL_ROWS; i++)
        {
            w.observe(Util.bytes(String.valueOf(i % CARDINALITY)));
            w.incrementRowId();
        }
        w.close();
        File file = new File(desc.filenameFor(w.component));
        file.deleteOnExit();
        
        // manually open the output component, and ensure that all is in order
        DataFileReader<BinSegment> reader = new DataFileReader<BinSegment>(file, new SpecificDatumReader<BinSegment>());
        ReusableBinSegment segment = ReusableBinSegment.poolGet();
        long lastRowid = -1;
        while (reader.hasNext())
        {
            segment.readFrom(reader);
            if (segment.header != null)
            {
                // header segment
                lastRowid = -1;
                continue;
            }

            // data segment
            assert segment.data.rowid > lastRowid : segment.data.rowid + " vs " + lastRowid;
            lastRowid = segment.data.rowid;
        }
        ReusableBinSegment.poolReturn(segment);
        reader.close();
    }
}
