/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import org.apache.cassandra.thrift.*;

import org.apache.cassandra.tools.NodeProbe;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MutationTest extends BaseTest
{
    @Test
    public void testInsert() throws Exception
    {
        List<InetAddress> hosts = getHosts();
        Cassandra.Client client = createClient(hosts.get(0));

        client.setKeyspace("Keyspace1");
        String key = createTemporaryKey();

        ColumnParent     cp = ColumnParent("Standard1");
        ConsistencyLevel cl = ConsistencyLevel.ONE;
        client.insert(key, cp, Column("c1", "v1", 0), cl);
        client.insert(key, cp, Column("c2", "v2", 0), cl);

        Thread.sleep(100);

        // verify get
        assertEquals(
            client.get(key, ColumnPath("Standard1", "c1"), cl).column,
            Column("c1", "v1", 0))

        // verify slice

    }
}
