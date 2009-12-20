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
package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.service.StorageService;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class SSTableTest extends CleanupHelper
{
    @Test
    public void testSingleWrite() throws IOException {
        // write test data
        String key = Integer.toString(1);
        byte[] byteval = new byte[1024];
        new Random().nextBytes(byteval);
        Column col = new Column("1".getBytes(), byteval, System.currentTimeMillis());

        TreeMap<ColumnKey, Column> map =
            new TreeMap<ColumnKey, Column>(ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME));
        map.put(new ColumnKey(StorageService.getPartitioner().decorateKey(key),
                              key.getBytes()),
                col);
        SSTableReader ssTable = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME, SSTableUtils.CFNAME, map);

        // verify
        verifySingle(ssTable, byteval, key);
        SSTableReader.reopenUnsafe(); // force reloading the index
        verifySingle(ssTable, byteval, key);
    }

    private void verifySingle(SSTableReader sstable, byte[] bytes, String key) throws IOException
    {
        throw new RuntimeException("Not implemented"); // FIXME
    }

    @Test
    public void testManyWrites() throws IOException {
        TreeMap<ColumnKey, Column> map =
            new TreeMap<ColumnKey, Column>(ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME));
        for ( int i = 100; i < 1000; ++i )
        {
            ColumnKey key = new ColumnKey(StorageService.getPartitioner().decorateKey(Integer.toString(i)),
                                          Integer.toString(i).getBytes());
            map.put(key, new Column((i + "").getBytes(),
                                    ("Avinash Lakshman is a good man: " + i).getBytes(),
                                    System.currentTimeMillis()));
        }

        // write
        SSTableReader ssTable = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME, SSTableUtils.CFNAME, map);

        // verify
        verifyMany(ssTable, map);
        SSTableReader.reopenUnsafe(); // force reloading the index
        verifyMany(ssTable, map);
    }

    private void verifyMany(SSTableReader sstable, TreeMap<ColumnKey, Column> map) throws IOException
    {
        throw new RuntimeException("Not implemented"); // FIXME
    }

    @Test
    public void testGetIndexedDecoratedKeysFor() throws Exception
    {
        final int numkeys = 1000;
        final int colsPerKey = 5;
        final byte[] columnVal = "This is the value of the columns!".getBytes();
        int columnBytes = 0;
        final String magic = "MAGIC!";

        TreeMap<ColumnKey, Column> map =
            new TreeMap<ColumnKey, Column>(ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME));
        for (int i = 0; i < numkeys; i++)
        {
            String stringKey = magic + Integer.toString(i);
            DecoratedKey decKey = StorageService.getPartitioner().decorateKey(stringKey);
            // write colsPerKey columns
            for (int k = 0; k < colsPerKey; k++)
            {
                Column col = new Column((k + "").getBytes(), columnVal,
                                        System.currentTimeMillis());
                map.put(new ColumnKey(decKey, Integer.toString(k).getBytes()),
                        col);
                columnBytes += col.size();
            }
        }

        // write
        Thread.sleep(1000); // FIXME
        long start = System.currentTimeMillis();
        SSTableReader ssTable = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME,
                                                             SSTableUtils.CFNAME, map);
        System.out.println("Wrote " + numkeys + " * " + colsPerKey + " in " + (System.currentTimeMillis() - start) + "ms.");

        // verify
        Predicate<SSTable> cfpred;
        Predicate<DecoratedKey> dkpred;

        cfpred = new Predicate<SSTable>() {
            public boolean apply(SSTable ss)
            {
                return ss.getColumnFamilyName().equals(SSTableUtils.CFNAME);
            }
            };
        // only accept keys that begin with 'magic'
        dkpred = new Predicate<DecoratedKey>() {
            public boolean apply(DecoratedKey key)
            {
                return key.key.startsWith(magic);
            }
            };
        int actual = SSTableReader.getIndexedDecoratedKeysFor(cfpred, dkpred).size();
        assert 0 < actual;

        // the number of keys in memory should be approximately:
        // numkeys * bytes_per_key / SSTWriter.TARGET_MAX_BLOCK_BYTES / SSTable.INDEX_INTERVAL
        double numSlices = (double)columnBytes / SSTableWriter.TARGET_MAX_BLOCK_BYTES;
        double expected = Math.ceil(numSlices / SSTable.INDEX_INTERVAL);
        assert actual <= expected : "actual " + actual + " !<= expected " + expected;
    }
}
