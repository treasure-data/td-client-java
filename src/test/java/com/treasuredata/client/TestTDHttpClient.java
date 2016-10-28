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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.treasuredata.client.model.TDApiErrorMessage;
import org.eclipse.jetty.client.HttpContentResponse;
import org.eclipse.jetty.client.HttpResponse;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestTDHttpClient
{
    private static Logger logger = LoggerFactory.getLogger(TestTDHttpClient.class);
    private TDHttpClient client;

    @Before
    public void setUp()
            throws Exception
    {
        client = TDClient.newClient().httpClient;
    }

    @After
    public void tearDown()
            throws Exception
    {
        client.close();
    }

    @Test
    public void parseInvalidErrorMessage()
    {
        Optional<TDApiErrorMessage> err = client.parseErrorResponse("{invalid json response}".getBytes(StandardCharsets.UTF_8));
        assertFalse(err.isPresent());
    }

    @Test
    public void addHttpRequestHeader()
    {
        TDApiRequest req = TDApiRequest.Builder.GET("/v3/system/server_status").addHeader("TEST_HEADER", "hello td-client-java").build();
        Response resp = client.submitRequest(req, Optional.<String>absent(), new TDHttpClient.DefaultContentHandler());
    }

    @Test
    public void deleteMethodTest()
    {
        try {
            TDApiRequest req = TDApiRequest.Builder.DELETE("/v3/dummy_endpoint").build();
            Response resp = client.submitRequest(req, Optional.<String>absent(), new TDHttpClient.DefaultContentHandler());
        }
        catch (TDClientHttpException e) {
            logger.warn("error", e);
        }
    }

    @Test
    public void retryOn429()
            throws Exception
    {
        // Configure an artificially low retry interval so we can measure with some confidence that Retry-After is respected
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(100)
                .setRetryLimit(1000)
                .build()
                .httpClient;

        final AtomicLong firstRequestNanos = new AtomicLong();
        final AtomicLong secondRequestNanos = new AtomicLong();
        final AtomicInteger requests = new AtomicInteger();

        final TDApiRequest req = TDApiRequest.Builder.GET("/v3/system/server_status").build();
        final byte[] body = "foobar".getBytes("UTF-8");
        final long retryAfterSeconds = 5;

        ContentResponse resp = client.submitRequest(req, Optional.<String>absent(), new TDHttpClient.DefaultContentHandler()
        {
            @Override
            public ContentResponse submit(Request request)
                    throws InterruptedException, ExecutionException, TimeoutException
            {
                List<Response.ResponseListener> listeners = ImmutableList.of();
                switch (requests.incrementAndGet()) {
                    case 1: {
                        firstRequestNanos.set(System.nanoTime());
                        HttpResponse response = new HttpResponse(request, listeners)
                                .status(429);
                        response.getHeaders().add("Retry-After", Long.toString(retryAfterSeconds));
                        return new HttpContentResponse(response, new byte[] {}, "", "");
                    }
                    case 2: {
                        secondRequestNanos.set(System.nanoTime());
                        HttpResponse response = new HttpResponse(request, listeners)
                                .status(200);
                        return new HttpContentResponse(response, body, "plain/text", "UTF-8");
                    }
                    default:
                        throw new AssertionError();
                }
            }
        });

        assertThat(requests.get(), is(2));
        assertThat(resp.getStatus(), is(200));
        assertThat(resp.getContent(), is(body));

        long delayNanos = secondRequestNanos.get() - firstRequestNanos.get();
        assertThat(delayNanos, Matchers.greaterThanOrEqualTo(SECONDS.toNanos(retryAfterSeconds)));
    }

    @Test
    public void retryOn429WithoutRetryAfter()
            throws Exception
    {
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(100)
                .setRetryLimit(3)
                .build()
                .httpClient;

        int requests = failWith429(Optional.<String>absent(), Optional.<Long>absent());

        assertThat(requests, is(4));
    }

    @Test
    public void retryOn429WithInvalidRetryAfter()
            throws Exception
    {
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(100)
                .setRetryLimit(3)
                .build()
                .httpClient;

        int requests = failWith429(Optional.of("foobar"), Optional.<Long>absent());

        assertThat(requests, is(4));
    }

    @Test
    public void failOn429_TimeLimitExceeded()
            throws Exception
    {
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(1000)
                .setRetryLimit(3)
                .build()
                .httpClient;

        // A high Retry-After value to verify that the exception is propagated without any retries when
        // the Retry-After value exceeds the configured retryLimit * retryMaxInterval
        final long retryAfterSeconds = 4711;

        int requests = failWith429(Optional.of(Long.toString(retryAfterSeconds)), Optional.of(retryAfterSeconds));

        // Verify that only one attempt was made
        assertThat(requests, is(1));
    }

    @Test
    public void failOn429_RetryLimitExceeded()
            throws Exception
    {
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(Integer.MAX_VALUE)
                .setRetryLimit(3)
                .build()
                .httpClient;

        long retryAfterSeconds = 1;

        int requests = failWith429(Optional.of(Long.toString(retryAfterSeconds)), Optional.of(retryAfterSeconds));

        // Verify that 4 attempts were made (original request + three retries)
        assertThat(requests, is(4));
    }

    private int failWith429(final Optional<String> retryAfterSeconds, final Optional<Long> expectedRetryAfterSeconds)
    {
        final AtomicInteger requests = new AtomicInteger();

        final TDApiRequest req = TDApiRequest.Builder.GET("/v3/system/server_status").build();

        try {
            client.submitRequest(req, Optional.<String>absent(), new TDHttpClient.DefaultContentHandler()
            {
                @Override
                public ContentResponse submit(Request request)
                        throws InterruptedException, ExecutionException, TimeoutException
                {
                    requests.incrementAndGet();
                    List<Response.ResponseListener> listeners = ImmutableList.of();
                    HttpResponse response = new HttpResponse(request, listeners)
                            .status(429);
                    if (retryAfterSeconds.isPresent()) {
                        response.getHeaders().add("Retry-After", retryAfterSeconds.get());
                    }
                    return new HttpContentResponse(response, new byte[] {}, "", "");
                }
            });

            fail();
        }
        catch (TDClientException e) {
            if (!(e instanceof TDClientHttpTooManyRequestsException)) {
                fail("Expected " + TDClientHttpTooManyRequestsException.class + ", got " + e.getClass());
            }
            TDClientHttpTooManyRequestsException tooManyRequestsException = (TDClientHttpTooManyRequestsException) e;
            assertThat(tooManyRequestsException.getRetryAfterSeconds(), is(expectedRetryAfterSeconds));
        }

        return requests.get();
    }
}
