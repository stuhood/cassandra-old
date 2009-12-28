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
        // values to be written
        TreeMap<ColumnKey, Column> map = new TreeMap<ColumnKey, Column>(COMPARATOR);
        // values to be confirmed per key (the dk of the ck is ignored)
        TreeSet<ColumnKey> values = new TreeSet<ColumnKey>(COMPARATOR);

        ColumnKey midish = null;
        final int numkeys = 10;
        final int colsperkey = 1000;
        Set<DecoratedKey> dks = new HashSet<DecoratedKey>();
        for (int i = 0; i < numkeys; i++)
        {
            values.clear();
            DecoratedKey dk = StorageService.getPartitioner().decorateKey("!" + i);
            for (int j = 0; j < colsperkey; j++)
            {
                byte[] bytes = Integer.toString(j).getBytes();
                ColumnKey key = new ColumnKey(dk, bytes);
                values.add(key);
                map.put(key, new Column(bytes, bytes, System.currentTimeMillis()));
                if (j == colsperkey / 2)
                    midish = key;
            }
            dks.add(dk);
        }

        // write
        SSTableReader reader = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME,
                                                            SSTableUtils.CFNAME, map);


        // for each key, select slices of the input columns, and confirm that they
        // match the expected names/values
        NavigableSet<ColumnKey> expected;
        SSTableSliceIterator ni;
        for (DecoratedKey dk : dks)
        {
            // collect right unbounded
            ni = new SSTableSliceIterator(reader, dk.key,
                                          midish.name(1), ColumnKey.NAME_END, false);
            expected = values.tailSet(midish, true);
            assertMatch(dk, expected, ni);

            // collect left unbounded
            ni = new SSTableSliceIterator(reader, dk.key,
                                          ColumnKey.NAME_BEGIN, midish.name(1), false);
            expected = values.headSet(midish, true);
            assertMatch(dk, expected, ni);

            // unbounded
            ni = new SSTableSliceIterator(reader, dk.key,
                                          ColumnKey.NAME_BEGIN, ColumnKey.NAME_END,
                                          false);
            expected = values;
            assertMatch(dk, expected, ni);
        }
    }

    @Test
    public void testIterateReversed() throws Exception
    {
        assert false : "FIXME: Not implemented"; // FIXME
    }

    public void assertMatch(DecoratedKey dk, Iterable<ColumnKey> expected, Iterator<IColumn> actual)
    {
        for (ColumnKey key : expected)
        {
            assert actual.hasNext() :
                "For " + dk + ", no match in iterator for " + new String(key.name(1));
            IColumn col = actual.next();
            // same bytes for key and value
            assert Arrays.equals(key.name(1), col.name()) : 
                new String(key.name(1)) + " != " + new String(col.name());
            assert Arrays.equals(key.name(1), col.value()) :
                new String(key.name(1)) + " != " + new String(col.value());
        }
        assert !actual.hasNext() :
            "For " + dk + ", iterator should be empty but contains at least " + actual.next();
    }
}
