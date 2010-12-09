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
import java.net.InetAddress;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.tools.NodeProbe;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public abstract class BaseTest
{
    protected static int THRIFT_PORT    = 9160;
    protected static int RPC_PORT       = 8080;

    protected static CassandraServiceController controller =
        CassandraServiceController.getInstance();
    protected NodeProbe probe;

    @BeforeClass
    public static void setUp() throws Exception
    {
        controller.ensureClusterRunning();

        List<InetAddress> hosts = controller.getHosts();
        probe = new NodeProbe(hosts.get(0), RPC_PORT);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        controller.shutdown();
    }

    protected String createTemporaryKey()
    {
        return String.format("test.key.%d", System.currentTimeMillis());
    }

    protected List<InetAddress> getReplicas(String keyspace, String key)
    {
        return probe.getEndPoints(keyspace, key);
    }

    protected Cassandra.Client createClient(String host) throws TTransportException, TException
    {
        TTransport transport    = new TSocket(host, THRIFT_PORT);
        TProtocol  protocol     = new TBinaryProtocol(transport);

        Cassandra.Client client = new Cassandra.Client(protocol);
        transport.open();

        return client;
    }
}
