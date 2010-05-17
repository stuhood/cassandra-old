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

import org.apache.cassandra.ASlice;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.db.*;

public class RowIndexedScannerTest extends RowIndexedTestBase
{
    protected void verifySingle(RowIndexedReader sstable, byte[] bytes, DecoratedKey key) throws IOException
    {
        RowIndexedScanner scanner = (RowIndexedScanner)sstable.getScanner(1024);
        scanner.first();
        // should contain a single slice
        ASlice sb = scanner.next();
        assertEquals(key, sb.begin.dk);
        assertEquals(key, sb.end.dk);

        List<Column> cols = sb.columns();
        assertEquals(1, cols.size());
        assert Arrays.equals(cols.get(0).value(), bytes);
    }

    protected void verifyMany(RowIndexedReader sstable, TreeMap<DecoratedKey, ColumnFamily> map) throws IOException
    {
        verifyManyForward(sstable, map);
        verifyManyRandom(sstable, map);
    }

    protected void verifyManyForward(RowIndexedReader sstable, TreeMap<DecoratedKey, ColumnFamily> map) throws IOException
    {
        RowIndexedScanner scanner = (RowIndexedScanner)sstable.getScanner(1024);
        scanner.first();
        Iterator<Map.Entry<DecoratedKey,ColumnFamily>> mapiter = map.entrySet().iterator();
        do
        {
            Map.Entry<DecoratedKey,ColumnFamily> entry = mapiter.next();

            // should contain a single slice
            ASlice sb = scanner.next();
            assertEquals(entry.getKey(), sb.begin.dk);
            assertEquals(entry.getKey(), sb.end.dk);

            List<Column> diskcols = sb.columns();
            assertEquals(entry.getValue().getSortedColumns().size(), diskcols.size());
            for (Column diskcol : diskcols)
            {
                IColumn expectedcol = entry.getValue().getColumn(diskcol.name());
                assert Arrays.equals(diskcol.value(), expectedcol.value());
            }
        }
        while (scanner.hasNext());

        assert !mapiter.hasNext() : "At least " + mapiter.next() + " remaining in iter.";
    }

    protected void verifyManyRandom(RowIndexedReader sstable, TreeMap<DecoratedKey, ColumnFamily> map) throws IOException
    {
        RowIndexedScanner scanner = (RowIndexedScanner)sstable.getScanner(1024);
        Iterator<Map.Entry<DecoratedKey,ColumnFamily>> mapiter = map.entrySet().iterator();
        do
        {
            Map.Entry<DecoratedKey,ColumnFamily> entry = mapiter.next();
            assert scanner.seekTo(entry.getKey());

            // should contain a single slice
            ASlice sb = scanner.next();
            assertEquals(entry.getKey(), sb.begin.dk);
            assertEquals(entry.getKey(), sb.end.dk);

            List<Column> diskcols = sb.columns();
            assertEquals(entry.getValue().getSortedColumns().size(), diskcols.size());
            for (Column diskcol : diskcols)
            {
                IColumn expectedcol = entry.getValue().getColumn(diskcol.name());
                assert Arrays.equals(diskcol.value(), expectedcol.value());
            }
        }
        while (scanner.hasNext());

        assert !mapiter.hasNext() : "At least " + mapiter.next() + " remaining in iter.";
    }

    protected void verifyManySuper(RowIndexedReader sstable, TreeMap<DecoratedKey, ColumnFamily> map) throws IOException
    {
        RowIndexedSuperScanner scanner = (RowIndexedSuperScanner)sstable.getScanner(1024);
        scanner.first();
        Iterator<Map.Entry<DecoratedKey,ColumnFamily>> mapiter = map.entrySet().iterator();
        do
        {
            Map.Entry<DecoratedKey,ColumnFamily> entry = mapiter.next();

            Iterator<IColumn> ecoliter = entry.getValue().getSortedColumns().iterator();
            do
            {
                ASlice sb = scanner.next();
                SuperColumn expectedcol = (SuperColumn)ecoliter.next();

                // should contain a slice per supercolumn
                assertEquals(entry.getKey(), sb.begin.dk);
                assertEquals(entry.getKey(), sb.end.dk);
                assert Arrays.equals(expectedcol.name(), sb.begin.name(1));
                assert Arrays.equals(expectedcol.name(), sb.end.name(1));

                List<Column> diskcols = sb.columns();
                assertEquals("" + diskcols, expectedcol.getSubColumns().size(), diskcols.size());
                for (Column diskcol : diskcols)
                {
                    IColumn expectedsubcol = expectedcol.getSubColumn(diskcol.name());
                    assert Arrays.equals(diskcol.value(), expectedsubcol.value());
                }
            }
            while(scanner.hasNext() && ecoliter.hasNext());
            assert !ecoliter.hasNext();
        }
        while (scanner.hasNext());

        assert !mapiter.hasNext() : "At least " + mapiter.next() + " remaining in iter.";
    }
}
