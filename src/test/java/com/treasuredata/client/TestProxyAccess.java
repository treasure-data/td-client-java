/*
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
 */
package com.treasuredata.client;

import com.treasuredata.client.model.TDJobList;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class TestProxyAccess
{
    private static Logger logger = LoggerFactory.getLogger(TestProxyAccess.class);
    private HttpProxyServer proxyServer;
    private int proxyPort;

    private static int findAvailablePort()
            throws IOException
    {
        ServerSocket socket = new ServerSocket(0);
        try {
            int port = socket.getLocalPort();
            return port;
        }
        finally {
            socket.close();
        }
    }

    private static final String PROXY_USER = "test";
    private static final String PROXY_PASS = "helloproxy";
    private AtomicInteger proxyAccessCount = new AtomicInteger(0);

    @Before
    public void setUp()
            throws Exception
    {
        proxyAccessCount.set(0);
        this.proxyPort = findAvailablePort();
        this.proxyServer = DefaultHttpProxyServer.bootstrap().withPort(proxyPort).withProxyAuthenticator(new ProxyAuthenticator()
        {
            @Override
            public boolean authenticate(String user, String pass)
            {
                boolean isValid = user.equals(PROXY_USER) && pass.equals(PROXY_PASS);
                return isValid;
            }
        }).withFiltersSource(new HttpFiltersSourceAdapter()
        {
            @Override
            public HttpFilters filterRequest(HttpRequest httpRequest, ChannelHandlerContext channelHandlerContext)
            {
                proxyAccessCount.incrementAndGet();
                return super.filterRequest(httpRequest, channelHandlerContext);
            }
        }).start();
    }

    @After
    public void tearDown()
            throws Exception
    {
        if (this.proxyServer != null) {
            proxyServer.stop();
        }
    }

    @Test
    public void proxyApiAccess()
    {
        ProxyConfig.ProxyConfigBuilder proxy = new ProxyConfig.ProxyConfigBuilder();
        proxy.setHost("localhost");
        proxy.setPort(proxyPort);
        proxy.setUser(PROXY_USER);
        proxy.setPassword(PROXY_PASS);
        TDClient client = new TDClient(TDClientConfig.currentConfig().withProxy(proxy.createProxyConfig()));
        try {
            TDJobList jobList = client.listJobs();
            logger.debug(jobList.toString());
            assertEquals(1, proxyAccessCount.get());
        }
        finally {
            logger.debug("proxy access count: {}", proxyAccessCount);
        }
    }
}