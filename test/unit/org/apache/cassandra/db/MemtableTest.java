package org.apache.cassandra.db;
/*
 * 
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
 * 
 */


import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.Util;
import org.junit.Test;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.SeekableScanner;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import static org.apache.cassandra.Util.column;

public class MemtableTest extends CleanupHelper
{
    @Test
    public void testScannerSeekSuper() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Super4");
        // we can't get access to the store's memtables, but we can create a new one!
        Memtable m = new Memtable(cfs);

        // create two supercolumns in a row
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Super4");
        SuperColumn sc1 = new SuperColumn("sc1".getBytes(), m.comp.typeAt(1));
        SuperColumn sc2 = new SuperColumn("sc2".getBytes(), m.comp.typeAt(1));
        sc1.addColumn(column("col1", "val1", 1L));
        sc2.addColumn(column("col2", "val1", 1L));
        cf.addColumn(sc1);
        cf.addColumn(sc2);
        m.put(Util.dk("key"), cf);

        // seek between the supercolumns
        ColumnKey sc2begin = new ColumnKey(Util.dk("key"), "sc2".getBytes(), ColumnKey.NAME_BEGIN);
        ColumnKey sc2end = new ColumnKey(Util.dk("key"), "sc2".getBytes(), ColumnKey.NAME_END);
        SeekableScanner scanner = m.getScanner();
        assert scanner.seekNear(sc2begin);
        assert scanner.hasNext();
        ASlice slice = scanner.next();
        assert cfs.comparator.compare(slice.begin, sc2begin) == 0;
        assert cfs.comparator.compare(slice.end, sc2end) == 0;
        assert !scanner.hasNext();
    }
}
