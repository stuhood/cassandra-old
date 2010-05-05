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

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.Util;

import org.apache.cassandra.ASlice;
import org.apache.cassandra.Scanner;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.db.*;

public class FilteredScannerTest extends CleanupHelper
{
    @Test
    public void testFilter() throws IOException
    {
        TreeMap<DecoratedKey, ColumnFamily> map = new TreeMap<DecoratedKey,ColumnFamily>();
        for (int i = 0; i < 10; i++)
        {
            byte[] bytes = ("Avinash Lakshman is a good man: " + i).getBytes();
            int byteHash = Arrays.hashCode(bytes);
            ColumnFamily cf = SSTableUtils.createCF(byteHash,
                                                    byteHash,
                                                    new Column(bytes, bytes, byteHash));
            map.put(Util.dk(Integer.toString(i)), cf);
        }

        // write
        SSTableReader ssTable = SSTableUtils.writeSSTable(map);
        String ksname = ssTable.getTableName();
        String cfname = ssTable.getColumnFamilyName();

        // verify with:
        // only the first key
        verifyFilter(ssTable, map.headMap(Util.dk("1")), QueryFilter.on(ksname, cfname).forKey(Util.dk("0")));
        // only the last key
        verifyFilter(ssTable, map.tailMap(Util.dk("9")), QueryFilter.on(ksname, cfname).forKey(Util.dk("9")));
    }

    protected void verifyFilter(SSTableReader sstable, SortedMap<DecoratedKey, ColumnFamily> map, QueryFilter qf) throws IOException
    {
        Scanner scanner = qf.filter(sstable.getScanner(1024));
        Iterator<Map.Entry<DecoratedKey,ColumnFamily>> mapiter = map.entrySet().iterator();
        while (scanner.hasNext() && mapiter.hasNext())
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

        assert !mapiter.hasNext() : "At least " + mapiter.next() + " remaining in expected.";
        assert !scanner.hasNext() : "At least " + scanner.next() + " remaining in actual.";
    }
}
