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

package org.apache.cassandra.db.filter;

import java.util.*;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableUtils;
import org.apache.cassandra.db.*;
import org.apache.cassandra.service.StorageService;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;
import static org.junit.Assert.*;

public class SSTableSliceIteratorTest
{
    public static final ColumnKey.Comparator COMPARATOR =
        ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME);

    @Test
    public void testIterateForward() throws Exception
    {
        ColumnKey midish = null;
        TreeMap<ColumnKey, Column> map = new TreeMap<ColumnKey, Column>(COMPARATOR);
        // add all names under a single large CF
        final int numkeys = 1000;
        DecoratedKey dk = StorageService.getPartitioner().decorateKey("bigrow!");
        for (int i = 0; i < numkeys; i++)
        {
            byte[] bytes = Integer.toString(i).getBytes();
            ColumnKey key = new ColumnKey(dk, bytes);
            map.put(key, new Column(bytes, bytes, System.currentTimeMillis()));
            if (i == numkeys / 2)
                midish = key;
        }

        // write
        SSTableReader reader = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME,
                                                            SSTableUtils.CFNAME, map);


        // select slices of the input columns, and confirm that they match the
        // iterator output
        NavigableSet<ColumnKey> expected;
        SSTableSliceIterator ni;

        // collect right unbounded
        ni = new SSTableSliceIterator(reader, dk.key,
                                      midish.name(1), ColumnKey.NAME_END, false);
        expected = map.tailMap(midish, true).navigableKeySet();
        assertMatch(expected, ni);

        // collect left unbounded
        ni = new SSTableSliceIterator(reader, dk.key,
                                      ColumnKey.NAME_BEGIN, midish.name(1), false);
        expected = map.headMap(midish, true).navigableKeySet();
        assertMatch(expected, ni);
    }

    @Test
    public void testIterateReverse() throws Exception
    {
        assert false : "FIXME: Not implemented"; // FIXME
    }

    public void assertMatch(Iterable<ColumnKey> expected, Iterator<IColumn> actual)
    {
        for (ColumnKey key : expected)
        {
            assert actual.hasNext() :
                "No match in iterator for " + new String(key.name(1));
            IColumn col = actual.next();
            // same bytes for key and value
            assert Arrays.equals(key.name(1), col.name()) : 
                new String(key.name(1)) + " != " + new String(col.name());
            assert Arrays.equals(key.name(1), col.value()) :
                new String(key.name(1)) + " != " + new String(col.value());
        }
        assert !actual.hasNext() :
            "Iterator should be empty but contains at least " + actual.next();
    }
}
