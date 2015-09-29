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

import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestServerFailures
{
    private static Logger logger = LoggerFactory.getLogger(TestServerFailures.class);
    Server server;
    ServerConnector http;
    int port;

    @Before
    public void setUp()
            throws Exception
    {
        server = new Server();
        port = TestProxyAccess.findAvailablePort();
        http = new ServerConnector(server);
        http.setHost("localhost");
        http.setPort(port);
        server.addConnector(http);
    }

    private void startServer()
            throws InterruptedException
    {
        final AtomicBoolean ready = new AtomicBoolean(false);
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    server.start();
                    ready.set(true);
                    server.join();
                }
                catch (Throwable e) {
                    throw Throwables.propagate(e);
                }
            }
        }).start();

        ExponentialBackOff backoff = new ExponentialBackOff(10, 1000, 1.5);
        while (!ready.get()) {
            Thread.sleep(backoff.nextWaitTimeMillis());
        }
    }

    @After
    public void tearDown()
            throws Exception
    {
        server.stop();
    }

    @Test
    public void jettyServerTest()
            throws Exception
    {
        final AtomicInteger accessCount = new AtomicInteger(0);
        server.setHandler(new AbstractHandler()
        {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException
            {
                logger.debug("request: " + request);
                accessCount.incrementAndGet();
                response.setStatus(HttpStatus.OK_200);
                response.getWriter().print("hello!");
                baseRequest.setHandled(true);
            }
        });
        startServer();
        String url = String.format("http://localhost:%s/v3/system/server_status", port);
        logger.info("url: " + url);
        try (InputStreamReader reader = new InputStreamReader(new URL(url).openStream())) {
            String content = CharStreams.toString(reader);
            logger.info(content);
        }
        assertEquals(1, accessCount.get());
    }

    @Test
    public void retryTest()
            throws Exception
    {
        logger.warn("Start request retry tests on 500 errors");
        final int retryLimit = 3;
        final AtomicInteger accessCount = new AtomicInteger(0);
        server.setHandler(new AbstractHandler()
        {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException
            {
                // Keep returning 500 error
                logger.debug("request: " + request);
                accessCount.incrementAndGet();
                response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                baseRequest.setHandled(true);
            }
        });
        startServer();

        TDClientConfig config = TDClientConfig
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .setRetryLimit(3)
                .build();
        TDClient client = new TDClient(config);
        try {
            client.serverStatus();
            fail("cannot reach here");
        }
        catch (TDClientHttpException e) {
            assertEquals(TDClientException.ErrorType.SERVER_ERROR, e.getErrorType());
            assertEquals(1 + retryLimit, accessCount.get());
        }
    }

    @Test
    public void unknownResponseCode()
            throws Exception
    {
        server.setHandler(new AbstractHandler()
        {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException
            {
                logger.debug("request: " + request);
                if (request.getRequestURI().endsWith("server_status")) {
                    response.setStatus(600); // return invalid response code
                }
                else {
                    response.setStatus(HttpStatus.NOT_ACCEPTABLE_406); // return invalid response code
                }
                baseRequest.setHandled(true);
            }
        });
        startServer();

        TDClientConfig config = TDClientConfig
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .build();
        TDClient client = new TDClient(config);
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
        server.setHandler(new AbstractHandler()
        {
            AtomicInteger count = new AtomicInteger(0);

            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                    throws IOException, ServletException
            {
                logger.debug("request: " + request);
                response.setStatus(HttpStatus.OK_200);
                response.setContentType("plain/text");
                if (count.get() == 0) {
                    response.getOutputStream().print("{broken json}");
                }
                else {
                    // invalid response
                    response.getOutputStream().print("{\"databases\":1}");
                }
                baseRequest.setHandled(true);
                count.incrementAndGet();
            }
        });
        startServer();

        TDClientConfig config = TDClientConfig
                .newBuilder()
                .setEndpoint("localhost")
                .setUseSSL(false)
                .setPort(port)
                .build();
        TDClient client = new TDClient(config);
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
}
