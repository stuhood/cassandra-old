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

public class SSTableNamesIteratorTest
{
    public static final ColumnKey.Comparator COMPARATOR =
        ColumnKey.getComparator(SSTableUtils.TABLENAME, SSTableUtils.CFNAME);

    @Test
    public void testIterate() throws Exception
    {
        // a subset of keys to collect
        SortedSet<ColumnKey> keys = new TreeSet<ColumnKey>(COMPARATOR);

        TreeMap<ColumnKey, Column> map = new TreeMap<ColumnKey, Column>(COMPARATOR);
        // add all names under a single large CF
        DecoratedKey dk = StorageService.getPartitioner().decorateKey("bigrow!");
        for (int i = 0; i < 1000; i++)
        {
            byte[] bytes = Integer.toString(i).getBytes();
            ColumnKey key = new ColumnKey(dk, bytes);
            map.put(key, new Column(bytes, bytes, System.currentTimeMillis()));

            if (i % 10 == 0)
                keys.add(key);
        }

        // write
        SSTableReader reader = SSTableUtils.writeRawSSTable(SSTableUtils.TABLENAME,
                                                            SSTableUtils.CFNAME, map);

        // collect
        SSTableNamesIterator ni = new SSTableNamesIterator(reader, keys);
        for (ColumnKey key : keys)
        {
            assert ni.hasNext() :
                "No match in iterator for " + new String(key.name(1));
            IColumn col = ni.next();
            // same bytes for key and value
            assert Arrays.equals(key.name(1), col.name()) : 
                new String(key.name(1)) + " != " + new String(col.name());
            assert Arrays.equals(key.name(1), col.value()) :
                new String(key.name(1)) + " != " + new String(col.value());
        }
        assert !ni.hasNext() :
            "Iterator should be empty but contains at least " + ni.next();
    }
}
