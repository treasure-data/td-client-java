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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.treasuredata.client.TDClientConfig.Type.API_ENDPOINT;
import static com.treasuredata.client.TDClientConfig.Type.API_PORT;
import static com.treasuredata.client.TDClientConfig.Type.CONNECTION_POOL_SIZE;
import static com.treasuredata.client.TDClientConfig.Type.CONNECT_TIMEOUT_MILLIS;
import static com.treasuredata.client.TDClientConfig.Type.PASSWORD;
import static com.treasuredata.client.TDClientConfig.Type.PROXY_HOST;
import static com.treasuredata.client.TDClientConfig.Type.PROXY_PASSWORD;
import static com.treasuredata.client.TDClientConfig.Type.PROXY_PORT;
import static com.treasuredata.client.TDClientConfig.Type.PROXY_USER;
import static com.treasuredata.client.TDClientConfig.Type.PROXY_USESSL;
import static com.treasuredata.client.TDClientConfig.Type.READ_TIMEOUT_MILLIS;
import static com.treasuredata.client.TDClientConfig.Type.RETRY_INITIAL_INTERVAL_MILLIS;
import static com.treasuredata.client.TDClientConfig.Type.RETRY_LIMIT;
import static com.treasuredata.client.TDClientConfig.Type.RETRY_MAX_INTERVAL_MILLIS;
import static com.treasuredata.client.TDClientConfig.Type.RETRY_MULTIPLIER;
import static com.treasuredata.client.TDClientConfig.Type.USER;
import static com.treasuredata.client.TDClientConfig.Type.USESSL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
@SuppressWarnings("unchecked")
public class TestTDClientConfig
{
    static Map<TDClientConfig.Type, Object> m;

    static {
        Map<TDClientConfig.Type, Object> p = new HashMap<>();
        p.put(API_ENDPOINT, "api2.treasuredata.com");
        p.put(API_PORT, 8981);
        p.put(USESSL, true);
        p.put(CONNECT_TIMEOUT_MILLIS, 2345);
        p.put(READ_TIMEOUT_MILLIS, 3456);
        p.put(CONNECTION_POOL_SIZE, 234);
        p.put(RETRY_LIMIT, 11);
        p.put(RETRY_INITIAL_INTERVAL_MILLIS, 456);
        p.put(RETRY_MAX_INTERVAL_MILLIS, 10000);
        p.put(RETRY_MULTIPLIER, 1.5);
        p.put(USER, "xxxx");
        p.put(PASSWORD, "yyyy");
        m = Collections.unmodifiableMap(p);

        assertTrue(new HashSet<>(TDClientConfig.knownProperties()).containsAll(m.keySet()));
    }

    private void validate(TDClientConfig config)
    {
        assertEquals(m.get(API_ENDPOINT), config.endpoint);
        assertEquals(m.get(API_PORT), config.port.get());
        assertEquals(m.get(USESSL), config.useSSL);
        assertEquals(m.get(CONNECT_TIMEOUT_MILLIS), config.connectTimeoutMillis);
        assertEquals(m.get(CONNECTION_POOL_SIZE), config.connectionPoolSize);
        assertEquals(m.get(READ_TIMEOUT_MILLIS), config.readTimeoutMillis);
        assertEquals(m.get(RETRY_INITIAL_INTERVAL_MILLIS), config.retryInitialIntervalMillis);
        assertEquals(m.get(RETRY_MAX_INTERVAL_MILLIS), config.retryMaxIntervalMillis);
        assertEquals((double) m.get(RETRY_MULTIPLIER), config.retryMultiplier, 0.001);
        assertEquals(m.get(RETRY_LIMIT), config.retryLimit);
        assertEquals(m.get(USER), config.user.get());
        assertEquals(m.get(PASSWORD), config.password.get());
        assertFalse(config.proxy.isPresent());
    }

    @Test
    public void testConfigParam()
    {
        // Configuration via Properties object
        Properties p = new Properties();
        for (Map.Entry<TDClientConfig.Type, Object> e : m.entrySet()) {
            p.setProperty(e.getKey().key, e.getValue().toString());
        }
        TDClient client = TDClient.newBuilder().setProperties(p).build();
        TDClientConfig config = client.config;
        validate(config);

        // Configuration via setters
        TDClientBuilder b = TDClient.newBuilder();
        b.setEndpoint(m.get(API_ENDPOINT).toString());
        b.setPort(Integer.parseInt(m.get(API_PORT).toString()));
        b.setUseSSL(Boolean.parseBoolean(m.get(USESSL).toString()));
        b.setConnectTimeoutMillis(Integer.parseInt(m.get(CONNECT_TIMEOUT_MILLIS).toString()));
        b.setConnectionPoolSize(Integer.parseInt(m.get(CONNECTION_POOL_SIZE).toString()));
        b.setReadTimeoutMillis(Integer.parseInt(m.get(READ_TIMEOUT_MILLIS).toString()));
        b.setRetryInitialIntervalMillis(Integer.parseInt(m.get(RETRY_INITIAL_INTERVAL_MILLIS).toString()));
        b.setRetryMaxIntervalMillis(Integer.parseInt(m.get(RETRY_MAX_INTERVAL_MILLIS).toString()));
        b.setRetryMultiplier(Double.parseDouble(m.get(RETRY_MULTIPLIER).toString()));
        b.setRetryLimit(Integer.parseInt(m.get(RETRY_LIMIT).toString()));
        b.setUser(m.get(USER).toString());
        b.setPassword(m.get(PASSWORD).toString());
        TDClientConfig config2 = b.build().config;
        validate(config2);
    }

    @Test
    public void testProxyParam()
    {
        Map<TDClientConfig.Type, Object> m = new HashMap<>();
        m.put(PROXY_HOST, "localhost1");
        m.put(PROXY_PORT, 8982);
        m.put(PROXY_USER, "pp");
        m.put(PROXY_PASSWORD, "xyz");
        m.put(PROXY_USESSL, true);
        Properties p = new Properties();
        for (Map.Entry<TDClientConfig.Type, Object> e : m.entrySet()) {
            p.setProperty(e.getKey().key, e.getValue().toString());
        }
        TDClientConfig config = TDClient.newBuilder().setProperties(p).build().config;
        ProxyConfig proxy = config.proxy.get();
        assertEquals(m.get(PROXY_HOST), proxy.getHost());
        assertEquals(m.get(PROXY_PORT), proxy.getPort());
        assertEquals(m.get(PROXY_USER), proxy.getUser().get());
        assertEquals(m.get(PROXY_USESSL), proxy.useSSL());
        assertEquals(m.get(PROXY_PASSWORD), proxy.getPassword().get());
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

    @Test
    public void readInvalidValue()
    {
        Assertions.assertThrows(TDClientException.class, () -> {
            Properties p = new Properties();
            p.setProperty(API_PORT.key, "xxxx");
            TDClient.newBuilder().setProperties(p);
        });
    }

    @Test
    public void readInvalidDoubleValue()
    {
        Assertions.assertThrows(TDClientException.class, () -> {
            Properties p = new Properties();
            p.setProperty(RETRY_MULTIPLIER.key, "xxx");
            TDClient.newBuilder().setProperties(p);
        });
    }

    @Test
    public void canConvertToProperties()
    {
        ProxyConfig.ProxyConfigBuilder pb = new ProxyConfig.ProxyConfigBuilder();
        pb.setHost("localhost");
        pb.setPort(8081);
        pb.setUser("dummy");
        pb.setPassword("hello");
        TDClientConfig config = TDClient.newBuilder(false).setProxy(pb.createProxyConfig()).buildConfig();
        Properties p1 = config.toProperties();
        TDClientConfig newConfig = TDClient.newBuilder(false).setProperties(p1).buildConfig();
        Properties p2 = newConfig.toProperties();
        assertEquals(p1, p2);
    }

    @Test
    public void customHeaders()
            throws Exception
    {
        Map<String, List<String>> noHeaders = Collections.emptyMap();
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("k1", Collections.singletonList("v1a"));
        headers.put("k2", Collections.singletonList("v2"));
        Map<String, List<String>> extraHeaders = new HashMap<>();
        headers.put("k1", Collections.singletonList("v1b"));
        headers.put("k3", Collections.singletonList("v3"));
        Map<String, List<String>> combinedHeaders = new HashMap<>(headers);
        for (Map.Entry<String, List<String>> e : extraHeaders.entrySet()) {
            combinedHeaders.compute(e.getKey(), (unused, col) -> {
                if (col != null) {
                    col.addAll(e.getValue());
                    return col;
                }
                else {
                    return new ArrayList<>(e.getValue());
                }
            });
        }

        TDClient clientWithNoHeaders1 = TDClient.newBuilder(false).build();
        TDClient clientWithNoHeaders2 = TDClient.newBuilder(false).setHeaders(noHeaders).build();
        TDClient clientWithHeaders = TDClient.newBuilder(false).setHeaders(headers).build();

        assertThat(clientWithNoHeaders1.httpClient.headers, is(equalTo(noHeaders)));
        assertThat(clientWithNoHeaders2.httpClient.headers, is(equalTo(noHeaders)));
        assertThat(clientWithHeaders.httpClient.headers, is(equalTo(headers)));
        assertThat(clientWithNoHeaders1.withHeaders(extraHeaders).httpClient.headers, is(equalTo(extraHeaders)));
        assertThat(clientWithNoHeaders1.withHeaders(extraHeaders).withHeaders(headers).httpClient.headers, is(equalTo(combinedHeaders)));
        assertThat(clientWithNoHeaders2.withHeaders(extraHeaders).httpClient.headers, is(equalTo(extraHeaders)));
        assertThat(clientWithNoHeaders2.withHeaders(extraHeaders).withHeaders(headers).httpClient.headers, is(equalTo(combinedHeaders)));
        assertThat(clientWithHeaders.withHeaders(extraHeaders).httpClient.headers, is(equalTo(combinedHeaders)));

        assertThat(clientWithNoHeaders1.httpClient.headers, is(noHeaders));
        assertThat(clientWithNoHeaders2.httpClient.headers, is(noHeaders));
        assertThat(clientWithHeaders.httpClient.headers, is(headers));
    }

    @Test
    public void apikeyWither()
            throws Exception
    {
        TDClientConfig config = TDClient.newBuilder().buildConfig();

        assertThat(config.withApiKey("foo").apiKey, is(Optional.of("foo")));
        assertThat(config.withApiKey(Optional.of("foo")).apiKey, is(Optional.of("foo")));
        assertThat(config.withApiKey(Optional.empty()).apiKey, is(Optional.empty()));
        assertThat(config.withApiKey("foo").withApiKey("bar").apiKey, is(Optional.of("bar")));
        assertThat(config.withApiKey("foo").withApiKey(Optional.empty()).apiKey, is(Optional.empty()));
        assertThat(config.withApiKey(Optional.empty()).withApiKey("bar").apiKey, is(Optional.of("bar")));
    }

    private Matcher<Map<String, ? extends Collection<String>>> equalTo(final Map<String, ? extends Collection<String>> multimap)
    {
        return new BaseMatcher<Map<String, ? extends Collection<String>>>()
        {
            @Override
            public boolean matches(Object item)
            {
                if (!(item instanceof Map)) {
                    return false;
                }
                Map<String, Collection<String>> other = (Map<String, Collection<String>>) item;
                return multimap.entrySet().equals(other.entrySet());
            }

            @Override
            public void describeTo(Description description)
            {
                description.appendValue(multimap.entrySet());
            }
        };
    }
}
