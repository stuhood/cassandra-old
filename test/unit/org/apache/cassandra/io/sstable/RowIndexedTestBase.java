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

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.db.*;

public abstract class RowIndexedTestBase extends CleanupHelper
{
    @Test
    public void testSingleWrite() throws IOException {
        // write test data
        String key = Integer.toString(1);
        byte[] bytes = new byte[1024];
        new Random().nextBytes(bytes);
        int byteHash = Arrays.hashCode(bytes);

        ColumnFamily cf = SSTableUtils.createCF(byteHash,
                                                byteHash,
                                                new Column(bytes, bytes, byteHash));

        TreeMap<String, ColumnFamily> map = new TreeMap<String,ColumnFamily>();
        map.put(key, cf);
        RowIndexedReader ssTable = (RowIndexedReader)SSTableUtils.writeSSTable(map);

        // verify
        verifySingle(ssTable, bytes, key);
        ssTable = (RowIndexedReader)SSTableReader.open(ssTable.getDescriptor()); // read the index from disk
        verifySingle(ssTable, bytes, key);
    }

    protected abstract void verifySingle(RowIndexedReader sstable, byte[] bytes, String key) throws IOException;

    @Test
    public void testManyWrites() throws IOException {
        TreeMap<String, ColumnFamily> map = new TreeMap<String,ColumnFamily>();
        for ( int i = 100; i < 1000; ++i )
        {
            byte[] bytes = ("Avinash Lakshman is a good man: " + i).getBytes();
            int byteHash = Arrays.hashCode(bytes);
            ColumnFamily cf = SSTableUtils.createCF(byteHash,
                                                    byteHash,
                                                    new Column(bytes, bytes, byteHash));

            map.put(Integer.toString(i), cf);
        }

        // write
        RowIndexedReader ssTable = (RowIndexedReader)SSTableUtils.writeSSTable(map);

        // verify
        verifyMany(ssTable, map);
        ssTable = (RowIndexedReader)SSTableReader.open(ssTable.getDescriptor()); // read the index from disk
        verifyMany(ssTable, map);
    }

    protected abstract void verifyMany(RowIndexedReader sstable, TreeMap<String, ColumnFamily> map) throws IOException;

    @Test
    public void testManySuperWrites() throws IOException {
        TreeMap<String, ColumnFamily> map = new TreeMap<String,ColumnFamily>();
        for ( int i = 100; i < 1000; ++i )
        {
            byte[] bytes = ("Avinash Lakshman is a good man: " + i).getBytes();
            int byteHash = Arrays.hashCode(bytes);
            SuperColumn sc = new SuperColumn(bytes,
                                             DatabaseDescriptor.getSubComparator(SSTableUtils.TABLENAME,
                                                                                 SSTableUtils.SUPERCFNAME));
            for (int k = 0; k < 10; ++k)
                sc.addColumn(new Column((""+k).getBytes(), bytes, k));
            ColumnFamily cf = SSTableUtils.createCF(SSTableUtils.TABLENAME, SSTableUtils.SUPERCFNAME, byteHash, byteHash, sc);

            map.put(Integer.toString(i), cf);
        }

        // write
        RowIndexedReader ssTable = (RowIndexedReader)SSTableUtils.writeSSTable(SSTableUtils.TABLENAME, SSTableUtils.SUPERCFNAME, map);

        // verify
        verifyManySuper(ssTable, map);
        ssTable = (RowIndexedReader)SSTableReader.open(ssTable.getDescriptor()); // read the index from disk
        verifyManySuper(ssTable, map);
    }

    protected abstract void verifyManySuper(RowIndexedReader sstable, TreeMap<String, ColumnFamily> map) throws IOException;
}
