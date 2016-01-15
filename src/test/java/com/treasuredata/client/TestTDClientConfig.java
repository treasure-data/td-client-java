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
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_INTERNAL_KEY;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_INTERNAL_KEY_VERSION;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PASSOWRD;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_HOST;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_PASSWORD;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_PORT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_USER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_USESSL;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_LIMIT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_MULTIPLIER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_USER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_USESSL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
@SuppressWarnings("unchecked")
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
        p.put(TD_CLIENT_RETRY_LIMIT, 11);
        p.put(TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS, 456);
        p.put(TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS, 10000);
        p.put(TD_CLIENT_RETRY_MULTIPLIER, 1.5);
        p.put(TD_CLIENT_USER, "xxxx");
        p.put(TD_CLIENT_PASSOWRD, "yyyy");
        p.put(TD_CLIENT_INTERNAL_KEY, "xxx-xxx-xxx");
        p.put(TD_CLIENT_INTERNAL_KEY_VERSION, "abcd");
        m = p.build();

        assertTrue(TDClientConfig.knownProperties.containsAll(m.keySet()));
    }

    private void validate(TDClientConfig config)
    {
        assertEquals(m.get(TD_CLIENT_API_ENDPOINT), config.endpoint);
        assertEquals(m.get(TD_CLIENT_API_PORT), config.port.get());
        assertEquals(m.get(TD_CLIENT_USESSL), config.useSSL);
        assertEquals(m.get(TD_CLIENT_CONNECT_TIMEOUT_MILLIS), config.connectTimeoutMillis);
        assertEquals(m.get(TD_CLIENT_CONNECTION_POOL_SIZE), config.connectionPoolSize);
        assertEquals(m.get(TD_CLIENT_IDLE_TIMEOUT_MILLIS), config.idleTimeoutMillis);
        assertEquals(m.get(TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS), config.retryInitialIntervalMillis);
        assertEquals(m.get(TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS), config.retryMaxIntervalMillis);
        assertEquals((double) m.get(TD_CLIENT_RETRY_MULTIPLIER), config.retryMultiplier, 0.001);
        assertEquals(m.get(TD_CLIENT_RETRY_LIMIT), config.retryLimit);
        assertEquals(m.get(TD_CLIENT_USER), config.user.get());
        assertEquals(m.get(TD_CLIENT_PASSOWRD), config.password.get());
        assertEquals(m.get(TD_CLIENT_INTERNAL_KEY), config.internalKey.get());
        assertEquals(m.get(TD_CLIENT_INTERNAL_KEY_VERSION), config.internalKeyVersion.get());
        assertFalse(config.proxy.isPresent());
    }

    @Test
    public void testConfigParam()
    {
        // Configuration via Properties object
        Properties p = new Properties();
        for (Map.Entry<String, Object> e : m.entrySet()) {
            p.setProperty(e.getKey(), e.getValue().toString());
        }
        TDClient client = TDClient.newBuilder().setProperties(p).build();
        TDClientConfig config = client.config;
        validate(config);

        // Configuration via setters
        TDClientBuilder b = TDClient.newBuilder();
        b.setEndpoint(m.get(TD_CLIENT_API_ENDPOINT).toString());
        b.setPort(Integer.parseInt(m.get(TD_CLIENT_API_PORT).toString()));
        b.setUseSSL(Boolean.parseBoolean(m.get(TD_CLIENT_USESSL).toString()));
        b.setConnectTimeoutMillis(Integer.parseInt(m.get(TD_CLIENT_CONNECT_TIMEOUT_MILLIS).toString()));
        b.setConnectionPoolSize(Integer.parseInt(m.get(TD_CLIENT_CONNECTION_POOL_SIZE).toString()));
        b.setIdleTimeoutMillis(Integer.parseInt(m.get(TD_CLIENT_IDLE_TIMEOUT_MILLIS).toString()));
        b.setRetryInitialIntervalMillis(Integer.parseInt(m.get(TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS).toString()));
        b.setRetryMaxIntervalMillis(Integer.parseInt(m.get(TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS).toString()));
        b.setRetryMultiplier(Double.parseDouble(m.get(TD_CLIENT_RETRY_MULTIPLIER).toString()));
        b.setRetryLimit(Integer.parseInt(m.get(TD_CLIENT_RETRY_LIMIT).toString()));
        b.setUser(m.get(TD_CLIENT_USER).toString());
        b.setPassword(m.get(TD_CLIENT_PASSOWRD).toString());
        b.setInternalKey(m.get(TD_CLIENT_INTERNAL_KEY).toString());
        b.setInternalKeyVersion(m.get(TD_CLIENT_INTERNAL_KEY_VERSION).toString());
        TDClientConfig config2 = b.build().config;
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
        TDClientConfig config = TDClient.newBuilder().setProperties(p).build().config;
        ProxyConfig proxy = config.proxy.get();
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

    @Test(expected = TDClientException.class)
    public void readInvalidValue()
    {
        Properties p = new Properties();
        p.setProperty(TD_CLIENT_API_PORT, "xxxx");
        TDClient.newBuilder().setProperties(p);
    }

    @Test(expected = TDClientException.class)
    public void readInvalidDoubleValue()
    {
        Properties p = new Properties();
        p.setProperty(TD_CLIENT_RETRY_MULTIPLIER, "xxx");
        TDClient.newBuilder().setProperties(p);
    }
}
