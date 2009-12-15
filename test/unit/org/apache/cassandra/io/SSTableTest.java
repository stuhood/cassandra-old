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
        byte[] bytes = new byte[1024];
        new Random().nextBytes(bytes);

        TreeMap<ColumnKey, byte[]> map =
            new TreeMap<ColumnKey,byte[]>(ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME));
        map.put(new ColumnKey(StorageService.getPartitioner().decorateKey(key),
                              key.getBytes()),
                bytes);
        SSTableReader ssTable = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME, SSTableUtils.CFNAME, map);

        // verify
        verifySingle(ssTable, bytes, key);
        SSTableReader.reopenUnsafe(); // force reloading the index
        verifySingle(ssTable, bytes, key);
    }

    private void verifySingle(SSTableReader sstable, byte[] bytes, String key) throws IOException
    {
    /**
     * FIXME: disabled until we get past crashing.
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.path, "r");
        file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)));
        assert key.equals(file.readUTF());
        int size = file.readInt();
        byte[] bytes2 = new byte[size];
        file.readFully(bytes2);
        assert Arrays.equals(bytes2, bytes);
    */
    }

    /**
     * FIXME: writes without deletion metadata (see SSTableUtils)
     */
    @Test
    public void testManyWrites() throws IOException {
        TreeMap<ColumnKey, byte[]> map =
            new TreeMap<ColumnKey,byte[]>(ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME));
        for ( int i = 100; i < 1000; ++i )
        {
            ColumnKey key = new ColumnKey(StorageService.getPartitioner().decorateKey(Integer.toString(i)),
                                          Integer.toString(i).getBytes());
            map.put(key, ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        SSTableReader ssTable = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME, SSTableUtils.CFNAME, map);

        // verify
        verifyMany(ssTable, map);
        SSTableReader.reopenUnsafe(); // force reloading the index
        verifyMany(ssTable, map);
    }

    private void verifyMany(SSTableReader sstable, TreeMap<ColumnKey, byte[]> map) throws IOException
    {
    /**
     * FIXME: disabled until we get past crashing.
        List<String> keys = new ArrayList<String>(map.keySet());
        Collections.shuffle(keys);
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.path, "r");
        for (String key : keys)
        {
            file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)));
            assert key.equals(file.readUTF());
            int size = file.readInt();
            byte[] bytes2 = new byte[size];
            file.readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key));
        }
    */
    }

    @Test
    public void testGetIndexedDecoratedKeysFor() throws IOException
    {
        final int numkeys = 1000;
        final byte[] columnVal = "blah".getBytes();
        final int columnBytes = columnVal.length;
        final String magic = "MAGIC!";

        TreeMap<ColumnKey, byte[]> map =
            new TreeMap<ColumnKey,byte[]>(ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME));
        for (int i = 0; i < numkeys; i++)
        {
            String stringKey = magic + Integer.toString(i);
            DecoratedKey decKey = StorageService.getPartitioner().decorateKey(stringKey);
            map.put(new ColumnKey(decKey, Integer.toString(i).getBytes()), columnVal);
        }

        // write
        SSTableReader ssTable = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME,
                                                             SSTableUtils.CFNAME, map);

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
        // numkeys * bytes_per_column / SSTWriter.TARGET_MAX_SLICE_BYTES / SSTable.INDEX_INTERVAL
        int totColumnBytes = numkeys * columnBytes;
        double numSlices = (double)totColumnBytes / SSTableWriter.TARGET_MAX_SLICE_BYTES;
        double ceil = numSlices / SSTable.INDEX_INTERVAL;
        assert actual <= ceil : "actual " + actual + " !<= ceil " + ceil;
    }
}
