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

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.Pair;

public class RowIndexedReaderTest extends RowIndexedTestBase
{
    protected void verifySingle(RowIndexedReader sstable, byte[] bytes, String key) throws IOException
    {
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)).position);

        Pair<DecoratedKey,ColumnFamily> row = deserialize(sstable, file);

        assertEquals(row.left.key, key);
        IColumn col = row.right.getColumn(bytes);
        assert col != null;
        assert Arrays.equals(bytes, col.value());
    }

    protected void verifyMany(RowIndexedReader sstable, TreeMap<String, ColumnFamily> map) throws IOException
    {
        List<String> keys = new ArrayList<String>(map.keySet());
        Collections.shuffle(keys);
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        for (String key : keys)
        {
            ColumnFamily expectedcf = map.get(key);
            file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)).position);

            Pair<DecoratedKey,ColumnFamily> row = deserialize(sstable, file);
            ColumnFamily diskcf = row.right;

            assertEquals(row.left.key, key);
            assertEquals(expectedcf.getSortedColumns().size(), diskcf.getSortedColumns().size());
            for (IColumn diskcol : diskcf.getSortedColumns())
            {
                IColumn expectedcol = expectedcf.getColumn(diskcol.name());
                assert Arrays.equals(diskcol.value(), expectedcol.value());
            }
        }
    }

    private Pair<DecoratedKey,ColumnFamily> deserialize(RowIndexedReader sstable, BufferedRandomAccessFile file) throws IOException
    {
        DecoratedKey dk = sstable.getPartitioner().convertFromDiskFormat(file.readUTF());
        file.readInt(); // row data size
        IndexHelper.defreezeBloomFilter(file);
        IndexHelper.deserializeIndex(file);

        ColumnFamily cf = ColumnFamily.serializer().deserializeFromSSTable(sstable, file);
        return new Pair<DecoratedKey,ColumnFamily>(dk, cf);
    }
}
