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
import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import org.apache.cassandra.thrift.*;

import org.apache.cassandra.tools.NodeProbe;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public abstract class TestBase
{
    protected static int CLIENT_PORT    = 9160;
    protected static int JMX_PORT       = 8080;

    protected static String KEYSPACE    = "KeyspaceTest1";

    protected static CassandraServiceController controller =
        CassandraServiceController.getInstance();
    protected static NodeProbe probe;

    private static void waitForDirective(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static void addKeyspace() throws Exception
    {
        List<CfDef> cfDefList = new LinkedList<CfDef>();

        CfDef standard1 = new CfDef(KEYSPACE, "Standard1");
        standard1.setComparator_type("BytesType");
        standard1.setKey_cache_size(10000);
        standard1.setRow_cache_size(1000);
        standard1.setRow_cache_save_period_in_seconds(0);
        standard1.setKey_cache_save_period_in_seconds(3600);
        standard1.setMemtable_flush_after_mins(59);
        standard1.setMemtable_throughput_in_mb(255);
        standard1.setMemtable_operations_in_millions(0.29);
        cfDefList.add(standard1);

        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = createClient(hosts.get(0).getHostAddress());

        client.system_add_keyspace(
            new KsDef(
                KEYSPACE,
                "org.apache.cassandra.locator.SimpleStrategy",
                3,
                cfDefList));

//TODO: IMPL: poll, until KS appears
        waitForDirective(10000);
    }

    protected static void dropKeyspace() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = createClient(hosts.get(0).getHostAddress());

        client.system_drop_keyspace(KEYSPACE);

//TODO: IMPL: poll, until KS leaves
        waitForDirective(10000);
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        controller.ensureClusterRunning();

        addKeyspace();

//        List<InetAddress> hosts = controller.getHosts();
//        probe = new NodeProbe(hosts.get(0).getHostAddress(), JMX_PORT);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        try
        {
            dropKeyspace();
        }
        finally
        {
            controller.shutdown();
        }
    }

    protected static String createTemporaryKey()
    {
        return String.format("test.key.%d", System.currentTimeMillis());
    }

    protected static List<InetAddress> getReplicas(String keyspace, String key)
    {
        return probe.getEndpoints(keyspace, key);
    }

    protected static Cassandra.Client createClient(String host)
        throws TTransportException, TException
    {
        TTransport transport    = new TSocket(host, CLIENT_PORT);
        transport               = new TFramedTransport(transport);
        TProtocol  protocol     = new TBinaryProtocol(transport);

        Cassandra.Client client = new Cassandra.Client(protocol);
        transport.open();

        return client;
    }
}
