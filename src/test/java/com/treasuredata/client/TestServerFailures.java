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

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
public class TestServerFailures
{
    private static final String CONTENT_TYPE = "Content-Type";

    private static Logger logger = LoggerFactory.getLogger(TestServerFailures.class);

    private MockWebServer server;
    private int port;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        port = TestProxyAccess.findAvailablePort();
        server = new MockWebServer();
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        server.close();
    }

    @Test
    public void mockServerTest()
            throws Exception
    {
        server.enqueue(new MockResponse().setBody("hello"));
        server.start(port);

        String url = String.format("http://localhost:%s/v3/system/server_status", port);
        logger.info("url: " + url);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(url).openStream()))) {
            String content = reader.lines().collect(Collectors.joining());
            logger.info(content);
        }
        assertEquals(1, server.getRequestCount());
    }

    @Test
    public void retryTest()
            throws Exception
    {
        logger.warn("Start request retry tests on 500 errors");
        final int retryLimit = 3;
        for (int i = 0; i < retryLimit + 1; ++i) {
            server.enqueue(new MockResponse().setResponseCode(500));
        }
        server.start(port);

        TDClient client = TDClient
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .setRetryLimit(3)
                .build();
        try {
            client.serverStatus();
            fail("cannot reach here");
        }
        catch (TDClientHttpException e) {
            assertEquals(TDClientException.ErrorType.SERVER_ERROR, e.getErrorType());
            assertEquals(1 + retryLimit, server.getRequestCount());
        }
    }

    @Test
    public void handleIdleTimeoutTest()
            throws Exception
    {
        logger.warn("Start request retry tests on idle timeout exception");
        handleTimeoutTest(TDClient
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .setReadTimeoutMillis(100)
                .setRetryLimit(1)
                .buildConfig()
        );
    }

    @Test
    public void handleRequestTimeoutTest()
            throws Exception
    {
        logger.warn("Start request retry tests on request timeout exception");
        handleTimeoutTest(TDClient
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .setReadTimeoutMillis(100)
                .setRetryLimit(1)
                .buildConfig()
        );
    }

    private void handleTimeoutTest(TDClientConfig config)
            throws Exception
    {
        server.enqueue(new MockResponse().setBody("{\"server\":\"ok\"}").throttleBody(5, 1, TimeUnit.SECONDS));
        server.enqueue(new MockResponse().setBody("{\"server\":\"ok\"}"));
        server.start(port);

        TDClient client = new TDClient(config);
        client.serverStatus();
        assertEquals(2, server.getRequestCount());
    }

    @Test
    public void handleEOFException()
            throws Exception
    {
        logger.warn("Start connection interruption test");
        // write intermediate result
        // Add long delay
        server.enqueue(new MockResponse().setBody("{\"server\": ").setBodyDelay(3, TimeUnit.SECONDS));
        server.start(port);

        TDClient client = TDClient
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .setRetryLimit(0)
                .build();

        ExecutorService t = Executors.newFixedThreadPool(2);
        t.submit(new Callable<Void>()
        {
            @Override
            public Void call()
                    throws Exception
            {
                try {
                    logger.info("check server status");
                    client.serverStatus();
                    fail("should not reach here");
                }
                catch (TDClientException e) {
                    logger.info("here: " + e.getMessage(), e);
                    if (e.getErrorType() == TDClientException.ErrorType.INTERRUPTED) {
                        assertThat(e.getRootCause().get(), anyOf(instanceOf(InterruptedException.class), instanceOf(EOFException.class)));
                    }
                    else {
                        assertEquals(TDClientException.ErrorType.SOCKET_ERROR, e.getErrorType());
                        assertTrue(e.getRootCause().get() instanceof ConnectException);
                    }
                    assertEquals(1, server.getRequestCount());
                }
                return null;
            }
        });
        t.submit(new Callable<Void>()
        {
            @Override
            public Void call()
                    throws Exception
            {
                // Close the server while sending the response
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                    logger.warn("Force closing a mock server");
                    server.shutdown();
                }
                catch (Exception e) {
                    logger.warn("failed to close server", e);
                }
                return null;
            }
        });
        t.shutdown();
        if (!t.awaitTermination(5, TimeUnit.SECONDS)) {
            fail("Gave up waiting MockServer termination");
        }
    }

    @Test
    public void unknownResponseCode()
            throws Exception
    {
        server.setDispatcher(new Dispatcher()
        {
            @Override
            public MockResponse dispatch(RecordedRequest request)
                    throws InterruptedException
            {
                if (request.getPath().endsWith("server_status")) {
                    return new MockResponse().setResponseCode(600); // return an invalid response code
                }
                else {
                    return new MockResponse().setResponseCode(HttpStatus.NOT_ACCEPTABLE_406); // return an invalid resonce code
                }
            }
        });
        server.start(port);

        TDClient client = TDClient.newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .build();
        try {
            client.serverStatus();
            fail("cannot reach here");
        }
        catch (TDClientHttpException e) {
            assertEquals(600, e.getStatusCode());
        }

        // 406 error
        try {
            client.listDatabaseNames();
            fail("cannot reach heer");
        }
        catch (TDClientHttpException e) {
            assertEquals(406, e.getStatusCode());
        }
    }

    @Test
    public void corruptedJsonResponse()
            throws Exception
    {
        server.enqueue(new MockResponse().setBody("{broken json}").setHeader(CONTENT_TYPE, "plain/text"));
        server.enqueue(new MockResponse().setBody("{\"database\":1}").setHeader(CONTENT_TYPE, "plain/text"));
        server.start(port);

        TDClient client = TDClient
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .build();
        try {
            client.listDatabaseNames();
            fail("cannot reach here");
        }
        catch (TDClientException e) {
            assertEquals(TDClientException.ErrorType.INVALID_JSON_RESPONSE, e.getErrorType());
        }

        try {
            client.listDatabaseNames();
            fail("cannot reach here");
        }
        catch (TDClientException e) {
            assertEquals(TDClientException.ErrorType.INVALID_JSON_RESPONSE, e.getErrorType());
        }
    }

    @Test
    public void errorBodyTimeoutRetryTest()
            throws Exception
    {
        logger.warn("Start request body timeout tests on 500 errors");
        final int retryLimit = 3;
        for (int i = 0; i < retryLimit + 1; ++i) {
            server.enqueue(new MockResponse().setResponseCode(500).setBody("{\"error\": \"server error\"}").setBodyDelay(1, TimeUnit.SECONDS));
        }
        server.enqueue(new MockResponse().setBody("{\"server\":\"ok\"}"));
        server.start(port);

        TDClient client = TDClient
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .setReadTimeoutMillis(100)
                .setRetryLimit(3)
                .build();
        try {
            client.serverStatus();
            fail("cannot reach here");
        }
        catch (TDClientHttpException e) {
            assertEquals(TDClientException.ErrorType.SERVER_ERROR, e.getErrorType());
            assertEquals(1 + retryLimit, server.getRequestCount());
        }

        server.shutdown();
    }

    @Test
    public void errorBodyTimeoutNonRetryTest()
            throws Exception
    {
        logger.warn("Start request body timeout tests on 404 errors");
        final int retryLimit = 3;
        for (int i = 0; i < retryLimit + 1; ++i) {
            server.enqueue(new MockResponse().setResponseCode(404).setBody("{\"error\": \"not found\"}").setBodyDelay(1, TimeUnit.SECONDS));
        }
        server.start(port);

        TDClient client = TDClient
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .setReadTimeoutMillis(100)
                .setRetryLimit(3)
                .build();
        try {
            client.serverStatus();
            fail("cannot reach here");
        }
        catch (TDClientHttpException e) {
            assertEquals(TDClientException.ErrorType.TARGET_NOT_FOUND, e.getErrorType());
            assertEquals(1, server.getRequestCount()); // no retry
        }

        server.shutdown();
    }
}
