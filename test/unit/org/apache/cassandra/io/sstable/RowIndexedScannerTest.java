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

public class RowIndexedScannerTest extends RowIndexedTestBase
{
    protected void verifySingle(RowIndexedReader sstable, byte[] bytes, String key) throws IOException
    {
        RowIndexedScanner scanner = (RowIndexedScanner)sstable.getScanner(1024);
        scanner.first();
        // should contain a single slice
        assertEquals(key, scanner.get().begin.dk.key);
        assertEquals(key, scanner.get().end.dk.key);

        List<Column> cols = scanner.getColumns();
        assertEquals(1, cols.size());
        assert Arrays.equals(cols.get(0).value(), bytes);
    }

    protected void verifyMany(RowIndexedReader sstable, TreeMap<String, ColumnFamily> map) throws IOException
    {
        RowIndexedScanner scanner = (RowIndexedScanner)sstable.getScanner(1024);
        scanner.first();
        Iterator<Map.Entry<String,ColumnFamily>> mapiter = map.entrySet().iterator();
        do
        {
            Map.Entry<String,ColumnFamily> entry = mapiter.next();

            // should contain a single slice
            assertEquals(entry.getKey(), scanner.get().begin.dk.key);
            assertEquals(entry.getKey(), scanner.get().end.dk.key);

            List<Column> diskcols = scanner.getColumns();
            assertEquals(entry.getValue().getSortedColumns().size(), diskcols.size());
            for (Column diskcol : diskcols)
            {
                IColumn expectedcol = entry.getValue().getColumn(diskcol.name());
                assert Arrays.equals(diskcol.value(), expectedcol.value());
            }
        }
        while (scanner.next());

        assert !mapiter.hasNext() : "At least " + mapiter.next() + " remaining in iter.";
    }

    protected void verifyManySuper(RowIndexedReader sstable, TreeMap<String, ColumnFamily> map) throws IOException
    {
        RowIndexedScanner scanner = (RowIndexedScanner)sstable.getScanner(1024);
        scanner.first();
        Iterator<Map.Entry<String,ColumnFamily>> mapiter = map.entrySet().iterator();
        do
        {
            Map.Entry<String,ColumnFamily> entry = mapiter.next();

            // should contain a single slice
            assertEquals(entry.getKey(), scanner.get().begin.dk.key);
            assertEquals(entry.getKey(), scanner.get().end.dk.key);

            Iterator<IColumn> ecoliter = entry.getValue().getSortedColumns().iterator();
            do
            {
                SuperColumn expectedcol = (SuperColumn)ecoliter.next();
                List<Column> diskcols = scanner.getColumns();
                assertEquals(expectedcol.getSubColumns().size(), diskcols.size());
                for (Column diskcol : diskcols)
                {
                    IColumn expectedsubcol = expectedcol.getSubColumn(diskcol.name());
                    assert Arrays.equals(diskcol.value(), expectedsubcol.value());
                }
            }
            while(scanner.next() && ecoliter.hasNext());
            assert !ecoliter.hasNext();
        }
        while (scanner.next());

        assert !mapiter.hasNext() : "At least " + mapiter.next() + " remaining in iter.";
    }
}
