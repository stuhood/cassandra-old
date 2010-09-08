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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.Util;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.bitidx.BitmapIndex;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableTest extends CleanupHelper
{
    public static final String KSNAME = "Keyspace1";
    public static final String INDEXEDCFNAME = "Indexed3";

    public static final String INDEXNAME = "state";

    public static final Map<String,String> STATES = new HashMap<String,String>();
    // row keys must be less than BitmapIndex.MAX_BINS to get test determinism
    public static final String CA = "" + (0);
    public static final String TX = "" + (BitmapIndex.MAX_BINS / 2);
    public static final String WA = "" + (BitmapIndex.MAX_BINS - 1);
    static
    {
        // row key -> column value
        STATES.put(CA, "CA");
        STATES.put(WA, "WA");
        STATES.put(TX, "TX");
    };

    @Test
    public void testSingleWrite() throws IOException {
        // write test data
        ByteBuffer key = ByteBuffer.wrap(Integer.toString(1).getBytes());
        ByteBuffer bytes = ByteBuffer.wrap(new byte[1024]);
        new Random().nextBytes(bytes.array());

        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer,ByteBuffer>();
        map.put(key, bytes);
        SSTableReader ssTable = SSTableUtils.writeRawSSTable("Keyspace1", "Standard1", map);

        // verify
        verifySingle(ssTable, bytes, key);
        ssTable = SSTableReader.open(ssTable.descriptor); // read the index from disk
        verifySingle(ssTable, bytes, key);
    }

    private void verifySingle(SSTableReader sstable, ByteBuffer bytes, ByteBuffer key) throws IOException
    {
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key), SSTableReader.Operator.EQ));
        assert key.equals(FBUtilities.readShortByteArray(file));
        int size = (int)SSTableReader.readRowSize(file, sstable.descriptor);
        byte[] bytes2 = new byte[size];
        file.readFully(bytes2);
        assert ByteBuffer.wrap(bytes2).equals(bytes);
    }

    @Test
    public void testManyWrites() throws IOException {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer,ByteBuffer>();
        for (int i = 100; i < 1000; ++i)
        {
            map.put(ByteBuffer.wrap(Integer.toString(i).getBytes()), ByteBuffer.wrap(("Avinash Lakshman is a good man: " + i).getBytes()));
        }

        // write
        SSTableReader ssTable = SSTableUtils.writeRawSSTable("Keyspace1", "Standard2", map);

        // verify
        verifyMany(ssTable, map);
        ssTable = SSTableReader.open(ssTable.descriptor); // read the index from disk
        verifyMany(ssTable, map);
    }

    private void verifyMany(SSTableReader sstable, Map<ByteBuffer, ByteBuffer> map) throws IOException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>(map.keySet());
        Collections.shuffle(keys);
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        for (ByteBuffer key : keys)
        {
            file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key), SSTableReader.Operator.EQ));
            assert key.equals( FBUtilities.readShortByteArray(file));
            int size = (int)SSTableReader.readRowSize(file, sstable.descriptor);
            byte[] bytes2 = new byte[size];
            file.readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key).array());
        }
    }

    /**
     * Populates an SSTable with exactly MAX_BINS unique values (including the given known values), so that we can
     * test deterministically: without knowing the bins, there is no way to guarantee that a key will be excluded.
     */
    private SSTableReader populate(Map<String,String> knowns) throws IOException
    {
        Map<String,ColumnFamily> rows = new TreeMap<String,ColumnFamily>();
        for (int j = 0; j < BitmapIndex.MAX_BINS; j++)
        {
            ColumnFamily cf = ColumnFamily.create(KSNAME, INDEXEDCFNAME);
            String key = String.valueOf(j);
            
            String known = knowns.get(key);
            cf.addColumn(Util.column(INDEXNAME, known != null ? known : String.valueOf(j)));
            rows.put(key, cf);
        }
        return SSTableUtils.writeSSTable(KSNAME, INDEXEDCFNAME, rows);
    }

    private Set<DecoratedKey> set(String... keys)
    {
        Set<DecoratedKey> set = new HashSet<DecoratedKey>();
        for (String key : keys)
            set.add(Util.dk(key));
        return set;
    }

    /**
     * Asserts all of the given rowkeys are returned for the index value and operator,
     * and that some rows have been eliminated by the index.
     */
    private void assertIndexed(SSTableReader sstable, IndexOperator op, Set<DecoratedKey> keys, String value) throws IOException
    {
        HashSet<DecoratedKey> expected = new HashSet<DecoratedKey>(keys);
        IndexExpression ex = new IndexExpression(INDEXNAME.getBytes(), op, value.getBytes());
        CloseableIterator<DecoratedKey> dks = sstable.scan(ex, Util.range("0", "99999"));
        boolean matched = false;
        int returned = 0;
        try
        {
            while (dks.hasNext())
            {
                expected.remove(dks.next());
                returned++;
            }
        }
        finally
        {
            dks.close();
        }
        assert expected.isEmpty() : expected + " not matched for " + op + "," + value + " in " + returned;
        assert returned < BitmapIndex.MAX_BINS : "returned >= limit for value " + value + " (expected " + keys + ")";
    }

    /**
     * Tests an index scan over a single index. Note that observing values in sorted order like
     * this is the worst case for a naive bin selection algorithm.
     */
    @Test
    public void testEqualsScan() throws IOException
    {
        // scan the "state" index for each state
        SSTableReader sstable = populate(STATES);
        for (Map.Entry<String,String> kv : STATES.entrySet())
            assertIndexed(sstable, IndexOperator.EQ, set(kv.getKey()), kv.getValue());
    }
}
