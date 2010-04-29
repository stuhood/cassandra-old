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

import org.apache.cassandra.Util;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class RowIndexedReaderTest extends RowIndexedTestBase
{
    @Test
    public void testSpannedIndexPositions() throws Exception
    {
        RowIndexedReader.BUFFER_SIZE = 40; // each index entry is ~11 bytes, so this will generate lots of spanned entries

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        // insert a bunch of data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 100; j += 2)
        {
            byte[] key = String.valueOf(j).getBytes();
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, "0".getBytes()), new byte[0], j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.submitMajor(store).get();

        // check that all our keys are found correctly
        RowIndexedReader sstable = (RowIndexedReader)store.getSSTables().iterator().next();
        for (int j = 0; j < 100; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            FileDataInput file = sstable.getFileDataInput(dk, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
            DecoratedKey keyInDisk = sstable.getPartitioner().convertFromDiskFormat(FBUtilities.readShortByteArray(file));
            assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getPath());
        }

        // check no false positives
        for (int j = 1; j < 110; j += 2)
        {
            DecoratedKey dk = Util.dk(String.valueOf(j));
            assert sstable.getPosition(dk) == null;
        }

        // check positionsize information
        assert sstable.indexSummary.getSpannedIndexDataPositions().entrySet().size() > 0;
        for (Map.Entry<IndexSummary.KeyPosition, SSTable.PositionSize> entry : sstable.indexSummary.getSpannedIndexDataPositions().entrySet())
        {
            IndexSummary.KeyPosition kp = entry.getKey();
            SSTable.PositionSize info = entry.getValue();

            long nextIndexPosition = kp.indexPosition + 2 + StorageService.getPartitioner().convertToDiskFormat(kp.key).length + 8;
            BufferedRandomAccessFile indexFile = new BufferedRandomAccessFile(sstable.indexFilename(), "r");
            indexFile.seek(nextIndexPosition);
            String nextKey = indexFile.readUTF();

            BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
            file.seek(info.position + info.size);
            assertEquals(nextKey, file.readUTF());
        }
    }

    protected void verifySingle(RowIndexedReader sstable, byte[] bytes, DecoratedKey key) throws IOException
    {
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        file.seek(sstable.getPosition(key).position);

        Pair<DecoratedKey,ColumnFamily> row = deserialize(sstable, file);

        assertEquals(row.left, key);
        IColumn col = row.right.getColumn(bytes);
        assert col != null;
        assert Arrays.equals(bytes, col.value());
        file.close();
    }

    protected void verifyMany(RowIndexedReader sstable, TreeMap<DecoratedKey, ColumnFamily> map) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<DecoratedKey>(map.keySet());
        Collections.shuffle(keys);
        for (DecoratedKey key : keys)
        {
            ColumnFamily expectedcf = map.get(key);
            FileDataInput file = sstable.getFileDataInput(key, 1024);

            Pair<DecoratedKey,ColumnFamily> row = deserialize(sstable, file);
            ColumnFamily diskcf = row.right;

            assertEquals(row.left, key);
            assertEquals(expectedcf.getSortedColumns().size(), diskcf.getSortedColumns().size());
            for (IColumn diskcol : diskcf.getSortedColumns())
            {
                IColumn expectedcol = expectedcf.getColumn(diskcol.name());
                assert Arrays.equals(diskcol.value(), expectedcol.value());
            }
            file.close();
        }
    }

    protected void verifyManySuper(RowIndexedReader sstable, TreeMap<DecoratedKey, ColumnFamily> map) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<DecoratedKey>(map.keySet());
        Collections.shuffle(keys);
        for (DecoratedKey key : keys)
        {
            ColumnFamily expectedcf = map.get(key);
            FileDataInput file = sstable.getFileDataInput(key, 1024);

            Pair<DecoratedKey,ColumnFamily> row = deserialize(sstable, file);
            ColumnFamily diskcf = row.right;

            assertEquals(row.left, key);
            assertEquals(expectedcf.getSortedColumns().size(), diskcf.getSortedColumns().size());
            for (IColumn diskcol : diskcf.getSortedColumns())
            {
                SuperColumn expectedcol = (SuperColumn)expectedcf.getColumn(diskcol.name());
                for (IColumn disksubcol : ((SuperColumn)diskcol).getSubColumns())
                {
                    Column expectedsubcol = (Column)expectedcol.getSubColumn(disksubcol.name());
                    assert Arrays.equals(disksubcol.value(), expectedsubcol.value());
                }
            }
            file.close();
        }
    }

    private Pair<DecoratedKey,ColumnFamily> deserialize(RowIndexedReader sstable, FileDataInput file) throws IOException
    {
        DecoratedKey dk = sstable.getPartitioner().convertFromDiskFormat(FBUtilities.readShortByteArray(file));
        file.readInt(); // row data size
        IndexHelper.defreezeBloomFilter(file);
        IndexHelper.deserializeIndex(file);

        ColumnFamily cf = ColumnFamily.serializer().deserializeFromSSTable(sstable, file);
        return new Pair<DecoratedKey,ColumnFamily>(dk, cf);
    }
}
