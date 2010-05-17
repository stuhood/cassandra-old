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
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class RowIndexedReaderTest extends RowIndexedTestBase
{
    protected void verifySingle(RowIndexedReader sstable, byte[] bytes, DecoratedKey key) throws IOException
    {
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        file.seek(sstable.getPosition(key).position);

        Pair<DecoratedKey,ColumnFamily> row = deserialize(sstable, file);

        assertEquals(row.left, key);
        IColumn col = row.right.getColumn(bytes);
        assert col != null;
        assert Arrays.equals(bytes, col.value());
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
