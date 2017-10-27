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
import com.treasuredata.client.model.TDTable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
@Ignore
public class TestSSLProxyAccess
{
    private static Logger logger = LoggerFactory.getLogger(TestProxyAccess.class);
    private HttpProxyServer proxyServer;
    private int proxyPort;
    private static final String PROXY_USER = "test";
    private static final String PROXY_PASS = "helloproxy";
    private AtomicInteger proxyAccessCount = new AtomicInteger(0);

    @Before
    public void setUp()
            throws Exception
    {
        proxyAccessCount.set(0);
        this.proxyPort = TestProxyAccess.findAvailablePort();
        this.proxyServer = DefaultHttpProxyServer
                .bootstrap()
                .withPort(proxyPort)
                .withProxyAuthenticator(new ProxyAuthenticator()
                {
                    @Override
                    public boolean authenticate(String user, String pass)
                    {
                        boolean isValid = user.equals(PROXY_USER) && pass.equals(PROXY_PASS);
                        logger.debug("Proxy Authentication: " + (isValid ? "success" : "failure"));
                        return isValid;
                    }
                    @Override
                    public String getRealm()
                    {
                        return null;
                    }
                })
                .withFiltersSource(new HttpFiltersSourceAdapter()
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
        proxy.useSSL(true);
        proxy.setHost("localhost");
        proxy.setPort(proxyPort);
        proxy.setUser(PROXY_USER);
        proxy.setPassword(PROXY_PASS);
        TDClient client = TDClient.newBuilder().setProxy(proxy.createProxyConfig()).build();
        try {
            client.serverStatus();

            List<TDTable> tableList = client.listTables("sample_datasets");
            assertTrue(tableList.size() >= 2);

            TDJobList jobList = client.listJobs();
            assertTrue(jobList.getJobs().size() > 0);
        }
        finally {
            logger.debug("proxy access count: {}", proxyAccessCount);
            assertEquals(1, proxyAccessCount.get());
        }
    }

    @Test
    public void wrongPassword()
    {
        ProxyConfig.ProxyConfigBuilder proxy = new ProxyConfig.ProxyConfigBuilder();
        proxy.setHost("localhost");
        proxy.useSSL(true);
        proxy.setPort(proxyPort);
        proxy.setUser(PROXY_USER);
        proxy.setPassword(PROXY_PASS + "---"); // Use an wrong password
        TDClient client = TDClient.newBuilder().setProxy(proxy.createProxyConfig()).build();
        try {
            client.listTables("sample_datasets");
            fail("should not reach here");
        }
        catch (TDClientException e) {
            logger.debug(e.getMessage());
            assertEquals(TDClientException.ErrorType.PROXY_AUTHENTICATION_FAILURE, e.getErrorType());
        }
    }
}
