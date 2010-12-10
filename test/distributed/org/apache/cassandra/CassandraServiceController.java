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

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceFactory;
import org.apache.whirr.service.cassandra.CassandraService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertThat;

public class CassandraServiceController
{
    private static final Logger LOG =
        LoggerFactory.getLogger(CassandraServiceController.class);

    private static final CassandraServiceController INSTANCE =
        new CassandraServiceController();
    
    public static CassandraServiceController getInstance()
    {
        return INSTANCE;
    }
    
    private boolean             running;
    private ClusterSpec         clusterSpec;
    private CassandraService    service;
    private Cluster             cluster;
    
    private CassandraServiceController()
    {
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
            throw new RuntimeException("FIXME: Must specify private key.");
        }

        Service s = new ServiceFactory().create(clusterSpec.getServiceName());
        assert s instanceof CassandraService;
        service = (CassandraService) s;
        
        cluster = service.launchCluster(clusterSpec);
        
        running = true;
    }

    public synchronized void shutdown() throws IOException, InterruptedException
    {
        LOG.info("Shutting down cluster...");
        service.destroyCluster(clusterSpec);
        running = false;
    }

    public List<InetAddress> getHosts()
    {
        Set<Cluster.Instance> instances = cluster.getInstances();
        List<InetAddress> hosts = new ArrayList<InetAddress>(instances.size());
        for (Cluster.Instance instance : instances)
            hosts.add(instance.getPublicAddress());
        return hosts;
    }
}
