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

import com.google.common.io.CharStreams;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDTable;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestProxyAccess
{
    private static Logger logger = LoggerFactory.getLogger(TestProxyAccess.class);
    private HttpProxyServer proxyServer;
    private int proxyPort;

    static int findAvailablePort()
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

    private ProxyConfig proxyBaseConfig()
    {
        ProxyConfig.ProxyConfigBuilder proxy = new ProxyConfig.ProxyConfigBuilder();
        proxy.setHost("localhost");
        proxy.setPort(proxyPort);
        proxy.setUser(PROXY_USER);
        proxy.setPassword(PROXY_PASS);
        return proxy.createProxyConfig();
    }

    @Test
    public void sslAccess()
            throws Exception
    {
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", proxyPort));
        Authenticator auth = new Authenticator()
        {
            @Override
            protected PasswordAuthentication getPasswordAuthentication()
            {
                return new PasswordAuthentication(PROXY_USER, PROXY_PASS.toCharArray());
            }
        };

        try {
            Authenticator.setDefault(auth);
            try (InputStream in = new URL("https://api.treasuredata.com/v3/system/server_status").openConnection(proxy).getInputStream()) {
                String ret = CharStreams.toString(new InputStreamReader(in));
                logger.info(ret);
            }
        }
        finally {
            Authenticator.setDefault(null);
        }
        assertEquals(1, proxyAccessCount.get());
    }

    @Test
    public void proxyApiAccess()
    {
        TDClient client = new TDClient(TDClientConfig.currentConfig().withProxy(proxyBaseConfig()));
        try {
            List<TDTable> tableList = client.listTables("sample_datasets");
            assertTrue(tableList.size() >= 2);
            assertEquals(1, proxyAccessCount.get());

            TDJobList jobList = client.listJobs();
            assertTrue(jobList.getJobs().size() > 0);
            assertEquals(2, proxyAccessCount.get());
        }
        finally {
            logger.debug("proxy access count: {}", proxyAccessCount);
        }
    }

    @Test
    public void wrongPassword()
    {
        ProxyConfig.ProxyConfigBuilder proxy = new ProxyConfig.ProxyConfigBuilder(proxyBaseConfig());
        proxy.setPassword(PROXY_PASS + "---"); // Use an wrong password
        TDClient client = new TDClient(TDClientConfig.currentConfig().withProxy(proxy.createProxyConfig()));
        try {
            client.listTables("sample_datasets");
            fail("should not reach here");
        }
        catch (TDClientException e) {
            logger.debug(e.getMessage());
            assertEquals(TDClientException.ErrorType.PROXY_AUTHENTICATION_FAILURE, e.getErrorType());
            assertEquals(1, proxyAccessCount.get());
        }
    }
}
