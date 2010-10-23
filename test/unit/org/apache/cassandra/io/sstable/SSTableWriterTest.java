package org.apache.cassandra.io.sstable;
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


import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.Util;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableWriterTest extends CleanupHelper {

    @Test
    public void testRecoverAndOpenKeys() throws IOException, ExecutionException, InterruptedException
    {
        testRecoverAndOpenIndexed("Keyspace1", "Indexed1");
    }

    @Test
    public void testRecoverAndOpenKeysBitmap() throws IOException, ExecutionException, InterruptedException
    {
        testRecoverAndOpenIndexed("Keyspace1", "Indexed3");
    }

    private void testRecoverAndOpenIndexed(String ksname, String cfname) throws IOException, ExecutionException, InterruptedException
    {
        // write a mutation managed by the CFS
        RowMutation rm = new RowMutation(ksname, ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath(cfname, null, ByteBufferUtil.bytes("birthdate")), FBUtilities.toByteBuffer(1L), 0);
        rm.apply();
        
        // write a manually managed sstable
        Map<String, ColumnFamily> entries = new HashMap<String, ColumnFamily>();
        ColumnFamily cf = ColumnFamily.create(ksname, cfname);        
        cf.addColumn(new Column(Util.bytes("birthdate"), FBUtilities.toByteBuffer(1L), 0));
        cf.addColumn(new Column(Util.bytes("anydate"), FBUtilities.toByteBuffer(1L), 0));
        entries.put("k2", cf);        
        
        cf = ColumnFamily.create(ksname, cfname);        
        cf.addColumn(new Column(Util.bytes("anydate"), FBUtilities.toByteBuffer(1L), 0));
        entries.put("k3", cf);
        
        SSTableReader orig = SSTableUtils.writeSSTable(ksname, cfname, entries);        
        // whack non-essential components to trigger the recover
        for (Component component : orig.components)
        {
            if (component.equals(Component.DATA))
                continue;
            FileUtils.deleteWithConfirm(orig.descriptor.filenameFor(component));
        }

        ColumnFamilyStore cfs = Table.open(ksname).getColumnFamilyStore(cfname);
        Descriptor desc = CompactionManager.instance.submitSSTableBuild(cfs, orig.descriptor, Component.INDEX_TYPES).get().left;
        SSTableReader sstr = SSTableReader.open(desc);
        cfs.addSSTable(sstr);
        cfs.rebuildSecondaryIndexes(Arrays.asList(sstr));
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes("birthdate"), IndexOperator.EQ, FBUtilities.toByteBuffer(1L));
        IndexClause clause = new IndexClause(Arrays.asList(expr), FBUtilities.EMPTY_BYTE_BUFFER, 100);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = cfs.scan(clause, range, filter);
        
        assertEquals("IndexExpression should return two rows on recoverAndOpen", 2, rows.size());
        assertTrue("First result should be 'k1'",ByteBufferUtil.bytes("k1").equals(rows.get(0).key.key));
    }
}
