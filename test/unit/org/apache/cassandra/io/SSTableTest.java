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

import java.io.IOException;
import java.util.*;

import org.junit.Test;
import static junit.framework.Assert.*;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.db.DecoratedKey;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class SSTableTest extends CleanupHelper
{
    public static final ColumnKey.Comparator COMPARATOR =
        ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME);

    @Test
    public void testSingleWrite() throws IOException {
        // write test data
        String strkey = Integer.toString(1);
        byte[] name = "1".getBytes();
        byte[] byteval = new byte[1024];
        new Random().nextBytes(byteval);
        ColumnKey key = new ColumnKey(StorageService.getPartitioner().decorateKey(strkey),
                                      name);
        Column col = new Column(name, byteval, System.currentTimeMillis());

        TreeMap<ColumnKey, Column> map = new TreeMap<ColumnKey, Column>(COMPARATOR);
        map.put(key, col);
        SSTableReader ssTable = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME, SSTableUtils.CFNAME, map);

        // verify
        verifySingle(ssTable, byteval, key);
        SSTableReader.reopenUnsafe(); // force reloading the index
        verifySingle(ssTable, byteval, key);
    }

    private void verifySingle(SSTableReader sstable, byte[] value, ColumnKey key) throws IOException
    {
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(),
                                                                     "r", 1 << 8);
        try
        {
            // single block
            SSTableReader.Block block = sstable.getBlock(file, 0);
            assert block.stream().available() > 0 :
                "Only " + block.stream().available() + " bytes available in block.";
            SSTable.SliceMark slice = SSTable.SliceMark.deserialize(block.stream());

            // single slice
            assert slice.length > 0;
            assertEquals(1, slice.numCols);
            assert slice.status == SSTable.SliceMark.BLOCK_END;
            assert COMPARATOR.compare(slice.key, key, 0) == 0 :
                "Natural slice boundary should have equal key";
            assert COMPARATOR.compare(slice.key, key, 1) != 0 :
                "Natural slice boundary should not have equal name";
            assert slice.nextKey == null;

            // single column
            Column column = Column.serializer().deserialize(block.stream());
            assert Arrays.equals(key.name(1), column.name());
            assert Arrays.equals(value, column.value());

            assert -1 == block.stream().read() : "Should have been at EOF";
        }
        finally
        {
            file.close();
        }
    }

    /**
     * Many slices, possibly in more than one block.
     */
    @Test
    public void testManyWrites() throws IOException {
        TreeMap<ColumnKey, Column> map = new TreeMap<ColumnKey, Column>(COMPARATOR);
        for ( int i = 100; i < 1000; ++i )
        {
            byte[] name = Integer.toString(i).getBytes();
            ColumnKey key = new ColumnKey(StorageService.getPartitioner().decorateKey(Integer.toString(i)),
                                          name);
            map.put(key, new Column(name,
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
        BufferedRandomAccessFile indexFile = new BufferedRandomAccessFile(sstable.indexFilename(), "r");
        BufferedRandomAccessFile dataFile = new BufferedRandomAccessFile(sstable.getFilename(),
                                                                     "r", 1 << 12);
        try
        {
            Iterator<Map.Entry<ColumnKey, Column>> columns = map.entrySet().iterator();
            SSTableReader.Block block = sstable.getBlock(dataFile, 0);
            IndexEntry indexEntry = IndexEntry.deserialize(indexFile);
            assert indexEntry.dataOffset == 0;

            SSTable.SliceMark slice,last = null;

            while (true)
            {
                slice = SSTable.SliceMark.deserialize(block.stream());

                if (last != null)
                {
                    assert COMPARATOR.compare(last.key, slice.key) < 0;
                    assert COMPARATOR.compare(last.end, slice.key) <= 0;
                    assert COMPARATOR.compare(last.nextKey, slice.key) == 0;
                }

                for (int i = 0; i < slice.numCols; i++)
                {
                    Column column = Column.serializer().deserialize(block.stream());
                    Map.Entry<ColumnKey,Column> colentry = columns.next();
                    assert Arrays.equals(colentry.getKey().name(1), column.name());
                    assert Arrays.equals(colentry.getValue().value(), column.value());
                }
                
                if (slice.nextKey == null)
                    break;
                if (slice.status == SSTable.SliceMark.BLOCK_END)
                {
                    indexEntry = IndexEntry.deserialize(indexFile);
                    assertEquals(indexEntry.dataOffset, dataFile.getFilePointer());
                    block = sstable.getBlock(dataFile, dataFile.getFilePointer());
                }
                last = slice;
            }

            assert !columns.hasNext();
        }
        finally
        {
            indexFile.close();
            dataFile.close();
        }
    }

    @Test
    public void testGetIndexedDecoratedKeysFor() throws Exception
    {
        final int numkeys = 1000;
        final int colsPerKey = 5;
        final byte[] columnVal = "This is the value of the columns!".getBytes();
        int columnBytes = 0;
        final String magic = "MAGIC!";

        TreeMap<ColumnKey, Column> map = new TreeMap<ColumnKey, Column>(COMPARATOR);
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
        // numkeys * bytes_per_key / SSTWriter.TARGET_MAX_BLOCK_BYTES / SSTable.INDEX_INTERVAL
        double numSlices = Math.ceil((double)columnBytes / SSTableWriter.TARGET_MAX_BLOCK_BYTES);
        double expected = 1 + Math.ceil(numSlices / SSTable.INDEX_INTERVAL);
        assert actual <= expected : "actual " + actual + " > expected " + expected;
    }
}
