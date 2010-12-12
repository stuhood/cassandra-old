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

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.KeyPair;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import static org.apache.whirr.service.RunUrlBuilder.runUrls;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceFactory;
import org.apache.whirr.service.cassandra.CassandraService;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.io.Payload;
import org.jclouds.ssh.ExecResponse;
import static org.jclouds.io.Payloads.newStringPayload;

import com.google.common.base.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertThat;

public class CassandraServiceController
{
    private static final Logger LOG =
        LoggerFactory.getLogger(CassandraServiceController.class);

    protected static int CLIENT_PORT    = 9160;
    protected static int JMX_PORT       = 8080;

    private static final CassandraServiceController INSTANCE =
        new CassandraServiceController();
    
    public static CassandraServiceController getInstance()
    {
        return INSTANCE;
    }
    
    private boolean     running;

    private ClusterSpec         clusterSpec;
    private CassandraService    service;
    private Cluster             cluster;
    private ComputeService      computeService;
    private Credentials         credentials;
    
    private CassandraServiceController()
    {
    }

    public Cassandra.Client createClient(InetAddress addr)
        throws TTransportException, TException
    {
        TTransport transport    = new TSocket(
                                    addr.getHostAddress(),
                                    CLIENT_PORT);
        transport               = new TFramedTransport(transport);
        TProtocol  protocol     = new TBinaryProtocol(transport);

        Cassandra.Client client = new Cassandra.Client(protocol);
        transport.open();

        return client;
    }

    private void waitForClusterInitialization()
    {
        for (Instance instance : cluster.getInstances())
            waitForNodeInitialization(instance.getPublicAddress());
    }
    
    private void waitForNodeInitialization(InetAddress addr)
    {
        while (true)
        {
            try
            {
                Cassandra.Client client = createClient(addr);

                client.describe_cluster_name();
                break;
            }
            catch (TException e)
            {
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ie)
                {
                    break;
                }
            }
        }
    }

    public synchronized void startup() throws Exception
    {
        LOG.info("Starting up cluster...");

        CompositeConfiguration config = new CompositeConfiguration();
        config.addConfiguration(new PropertiesConfiguration("whirr-default.properties"));
        if (System.getProperty("whirr.config") != null)
        {
            config.addConfiguration(
                new PropertiesConfiguration(System.getProperty("whirr.config")));
        }

        clusterSpec = new ClusterSpec(config);
        if (clusterSpec.getPrivateKey() == null)
        {
            Map<String, String> pair = KeyPair.generate();
            clusterSpec.setPublicKey(pair.get("public"));
            clusterSpec.setPrivateKey(pair.get("private"));
        }

        service = (CassandraService)new ServiceFactory().create(clusterSpec.getServiceName());
        cluster = service.launchCluster(clusterSpec);
        computeService = ComputeServiceContextBuilder.build(clusterSpec).getComputeService();
        // TODO: expose creds on CassandraService without this mumbo-jumbo
        NodeMetadata nm = computeService.getNodeMetadata(computeService.listNodes().iterator().next().getId());
        credentials = new Credentials(nm.getCredentials().identity, clusterSpec.readPrivateKey());

        waitForClusterInitialization();
        running = true;
    }

    public synchronized void shutdown() throws IOException, InterruptedException
    {
        LOG.info("Shutting down cluster...");
        if (service != null)
            service.destroyCluster(clusterSpec);
        running = false;
    }

    public synchronized boolean ensureClusterRunning() throws Exception
    {
        if (running)
        {
            LOG.info("Cluster already running.");
            return false;
        }
        else
        {
            startup();
            return true;
        }
    }

    public Failure failHosts(InetAddress... hosts)
    {
        return new Failure(hosts).trigger();
    }

    /** TODO: Move to CassandraService? */
    protected void callOnHost(InetAddress host, String payload)
    {
        final String hoststring = host.getHostAddress();
        Map<? extends NodeMetadata,ExecResponse> results;
        try
        {
            results = computeService.runScriptOnNodesMatching(new Predicate<NodeMetadata>()
            {
                public boolean apply(NodeMetadata node)
                {
                    return node.getPublicAddresses().contains(hoststring);
                }
            }, newStringPayload(runUrls(clusterSpec.getRunUrlBase(), payload)),
            RunScriptOptions.Builder.overrideCredentialsWith(credentials));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        if (results.size() != 1)
            throw new RuntimeException(results.size() + " hosts matched " + host + ": " + results);
        ExecResponse response = results.values().iterator().next();
        if (response.getExitCode() != 0)
            throw new RuntimeException("Call " + payload + " on " + host + " failed: " + response);
    }

    public List<InetAddress> getHosts()
    {
        Set<Instance> instances = cluster.getInstances();
        List<InetAddress> hosts = new ArrayList<InetAddress>(instances.size());
        for (Instance instance : instances)
            hosts.add(instance.getPublicAddress());
        return hosts;
    }

    class Failure
    {
        private InetAddress[] hosts;
        public Failure(InetAddress... hosts)
        {
            this.hosts = hosts;
        }
        
        public Failure trigger()
        {
            for (InetAddress host : hosts)
                callOnHost(host, "apache/cassandra/stop");
            return this;
        }

        public void resolve()
        {
            for (InetAddress host : hosts)
                callOnHost(host, "apache/cassandra/start");
            for (InetAddress host : hosts)
                waitForNodeInitialization(host);
        }
    }
}
