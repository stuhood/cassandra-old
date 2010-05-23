/*
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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

public class SegmentedFileTest extends CleanupHelper
{
    @Test
    public void testSegmentedFile() throws Exception
    {
        SegmentedFile.BUFFER_SIZE = 128; // generate lots of segments

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        // insert a bunch of data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        HashSet<String> keys = new HashSet<String>();
        for (int j = 0; j < 100; j += 2)
        {
            byte[] key = String.valueOf(j).getBytes();
            keys.add(String.valueOf(j));
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, "0".getBytes()), new byte[0], j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.submitMajor(store).get();
        SSTableReader sstable = store.getSSTables().iterator().next();

        // check that all our keys are found for random access
        for (String key : keys)
        {
            DecoratedKey dk = Util.dk(key);
            FileDataInput file = sstable.getFileDataInput(dk, 1024);
            DecoratedKey keyInDisk = sstable.getPartitioner().convertFromDiskFormat(FBUtilities.readShortByteArray(file));
            assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getPath());
        }
    }
}
