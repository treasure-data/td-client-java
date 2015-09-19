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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.treasuredata.client.TDClientConfig.TD_CLIENT_API_ENDPOINT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_API_PORT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_CONNECTION_POOL_SIZE;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_CONNECT_TIMEOUT_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_IDLE_TIMEOUT_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PASSOWRD;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_HOST;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_PASSWORD;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_PORT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_USER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_USESSL;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_INITIAL_WAIT_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_INTERVAL_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_LIMIT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_USER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_USESSL;
import static com.treasuredata.client.TDClientConfig.newConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestTDClientConfig
{
    static ImmutableMap<String, Object> m;

    static {
        ImmutableMap.Builder p = ImmutableMap.<String, Object>builder();
        p.put(TD_CLIENT_API_ENDPOINT, "api2.treasuredata.com");
        p.put(TD_CLIENT_API_PORT, 8981);
        p.put(TD_CLIENT_USESSL, true);
        p.put(TD_CLIENT_CONNECT_TIMEOUT_MILLIS, 2345);
        p.put(TD_CLIENT_IDLE_TIMEOUT_MILLIS, 3456);
        p.put(TD_CLIENT_CONNECTION_POOL_SIZE, 234);
        p.put(TD_CLIENT_RETRY_INITIAL_WAIT_MILLIS, 456);
        p.put(TD_CLIENT_RETRY_LIMIT, 11);
        p.put(TD_CLIENT_RETRY_INTERVAL_MILLIS, 3500);
        p.put(TD_CLIENT_USER, "xxxx");
        p.put(TD_CLIENT_PASSOWRD, "yyyy");
        m = p.build();
    }

    private void validate(TDClientConfig config)
    {
        assertEquals(m.get(TD_CLIENT_API_ENDPOINT), config.getEndpoint());
        assertEquals(m.get(TD_CLIENT_API_PORT), config.getPort());
        assertEquals(m.get(TD_CLIENT_USESSL), config.isUseSSL());
        assertEquals(m.get(TD_CLIENT_CONNECT_TIMEOUT_MILLIS), config.getConnectTimeoutMillis());
        assertEquals(m.get(TD_CLIENT_CONNECTION_POOL_SIZE), config.getConnectionPoolSize());
        assertEquals(m.get(TD_CLIENT_RETRY_INITIAL_WAIT_MILLIS), config.getRetryInitialWaitMillis());
        assertEquals(m.get(TD_CLIENT_RETRY_INTERVAL_MILLIS), config.getRetryIntervalMillis());
        assertEquals(m.get(TD_CLIENT_RETRY_LIMIT), config.getRetryLimit());
        assertEquals(m.get(TD_CLIENT_USER), config.getUser().get());
        assertEquals(m.get(TD_CLIENT_PASSOWRD), config.getPassword().get());
        assertFalse(config.getProxy().isPresent());
    }

    @Test
    public void testConfigParam()
    {
        // Configuration via Properties object
        Properties p = new Properties();
        for (Map.Entry<String, Object> e : m.entrySet()) {
            p.setProperty(e.getKey(), e.getValue().toString());
        }
        TDClientConfig config = newConfig(p);
        validate(config);

        // Configuration via setters
        TDClientConfig.Builder b = TDClientConfig.newBuilder();
        b.setEndpoint(m.get(TD_CLIENT_API_ENDPOINT).toString());
        b.setPort(Integer.parseInt(m.get(TD_CLIENT_API_PORT).toString()));
        b.setUseSSL(Boolean.parseBoolean(m.get(TD_CLIENT_USESSL).toString()));
        b.setConnectTimeoutMillis(Integer.parseInt(m.get(TD_CLIENT_CONNECT_TIMEOUT_MILLIS).toString()));
        b.setConnectionPoolSize(Integer.parseInt(m.get(TD_CLIENT_CONNECTION_POOL_SIZE).toString()));
        b.setRetryInitialWaitMillis(Integer.parseInt(m.get(TD_CLIENT_RETRY_INITIAL_WAIT_MILLIS).toString()));
        b.setRetryIntervalMillis(Integer.parseInt(m.get(TD_CLIENT_RETRY_INTERVAL_MILLIS).toString()));
        b.setRetryLimit(Integer.parseInt(m.get(TD_CLIENT_RETRY_LIMIT).toString()));
        b.setUser(m.get(TD_CLIENT_USER).toString());
        b.setPassword(m.get(TD_CLIENT_PASSOWRD).toString());
        TDClientConfig config2 = b.build();
        validate(config2);
    }

    @Test
    public void testProxyParam()
    {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put(TD_CLIENT_PROXY_HOST, "localhost1");
        m.put(TD_CLIENT_PROXY_PORT, 8982);
        m.put(TD_CLIENT_PROXY_USER, "pp");
        m.put(TD_CLIENT_PROXY_PASSWORD, "xyz");
        m.put(TD_CLIENT_PROXY_USESSL, true);
        Properties p = new Properties();
        for (Map.Entry<String, Object> e : m.entrySet()) {
            p.setProperty(e.getKey(), e.getValue().toString());
        }
        TDClientConfig config = newConfig(p);
        ProxyConfig proxy = config.getProxy().get();
        assertEquals(m.get(TD_CLIENT_PROXY_HOST), proxy.getHost());
        assertEquals(m.get(TD_CLIENT_PROXY_PORT), proxy.getPort());
        assertEquals(m.get(TD_CLIENT_PROXY_USER), proxy.getUser().get());
        assertEquals(m.get(TD_CLIENT_PROXY_USESSL), proxy.useSSL());
        assertEquals(m.get(TD_CLIENT_PROXY_PASSWORD), proxy.getPassword().get());
    }

    @Test
    public void readInvalidFile()
            throws Exception
    {
        String home = System.getProperty("user.home");
        try {
            System.setProperty("user.home", "/tmp");
            Properties p = TDClientConfig.readTDConf();
            assertTrue(p.isEmpty());
        }
        finally {
            System.setProperty("user.home", home);
        }

        // Reading a missing file causes IOException
        try {
            TDClientConfig.readTDConf(new File("target/missing-file-xxxxxxx"));
            fail("should not reach here");
        }
        catch (TDClientException e) {
            assertEquals(TDClientException.ErrorType.INVALID_CONFIGURATION, e.getErrorType());
        }
    }
}
