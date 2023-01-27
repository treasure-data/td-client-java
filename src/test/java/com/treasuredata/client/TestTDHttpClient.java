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

import com.google.common.collect.ImmutableMultimap;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.exparity.hamcrest.date.DateMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.treasuredata.client.TDHttpRequestHandlers.stringContentHandler;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.exparity.hamcrest.date.DateMatchers.within;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
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
    public void addHttpRequestHeader()
    {
        TDApiRequest req = TDApiRequest.Builder.GET("/v3/system/server_status").addHeader("TEST_HEADER", "hello td-client-java").build();
        String resp = client.submitRequest(req, Optional.empty(), stringContentHandler);
    }

    @Test
    public void addUserAgentHeader()
    {
        TDApiRequest apiRequest = TDApiRequest.Builder.GET("/v3/system/server_status").build();

        // Without specifying User-Agent header
        Request request1 = client.prepareRequest(apiRequest, Optional.empty());
        assertEquals("td-client-java unknown", request1.header(USER_AGENT));

        // With specifying User-Agent header
        TDHttpClient newClient = client.withHeaders(ImmutableMultimap.of(USER_AGENT, "td-sample-client 1.0"));
        Request request2 = newClient.prepareRequest(apiRequest, Optional.empty());
        assertEquals("td-client-java unknown,td-sample-client 1.0", request2.header(USER_AGENT));
    }

    @Test
    public void deleteMethodTest()
    {
        try {
            TDApiRequest req = TDApiRequest.Builder.DELETE("/v3/dummy_endpoint").build();
            String resp = client.submitRequest(req, Optional.empty(), stringContentHandler);
            fail();
        }
        catch (TDClientHttpException e) {
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

        byte[] result = client.submitRequest(req, Optional.empty(), new TDHttpRequestHandler<byte[]>()
        {
            @Override
            public Response send(OkHttpClient httpClient, Request request)
                    throws IOException
            {
                switch (requests.incrementAndGet()) {
                    case 1: {
                        firstRequestNanos.set(System.nanoTime());
                        return new Response.Builder()
                                .request(request)
                                .protocol(Protocol.HTTP_1_1)
                                .message("")
                                .code(429)
                                .header("Retry-After", Long.toString(retryAfterSeconds))
                                .body(ResponseBody.create(null, ""))
                                .build();
                    }
                    case 2: {
                        secondRequestNanos.set(System.nanoTime());
                        return new Response.Builder()
                                .request(request)
                                .protocol(Protocol.HTTP_1_1)
                                .message("")
                                .code(200)
                                .body(ResponseBody.create(MediaType.parse("plain/text"), body))
                                .build();
                    }
                    default:
                        throw new AssertionError();
                }
            }

            public byte[] onSuccess(Response response)
                    throws Exception
            {
                assertThat(response.code(), is(200));
                return response.body().bytes();
            }
        });

        assertThat(requests.get(), is(2));
        assertThat(result, is(body));

        // The delay often goes below 5 seconds fractionally, e.g. 4.9996 seconds (99.992%),
        // which appears to be acceptable. So 1% margin of error is allowed.
        long delayNanos = secondRequestNanos.get() - firstRequestNanos.get();
        long almostFiveSeconds = MILLISECONDS.toNanos(retryAfterSeconds * 990);
        assertThat(delayNanos, Matchers.greaterThanOrEqualTo(almostFiveSeconds));
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

        int requests = failWith429(Optional.empty(), Optional.empty());

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

        int requests = failWith429(Optional.of("foobar"), Optional.empty());

        assertThat(requests, is(4));
    }

    @Test
    public void failOn429_TimeLimitExceeded_Seconds()
            throws Exception
    {
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(1000)
                .setRetryLimit(3)
                .build()
                .httpClient;

        // A high Retry-After value to verify that the exception is propagated without any retries when
        // the Retry-After value exceeds the configured retryLimit * retryMaxInterval
        long retryAfterSeconds = 4711;

        Date expectedRetryAfter = new Date(System.currentTimeMillis() + SECONDS.toMillis(retryAfterSeconds));

        int requests = failWith429(
                Optional.of(Long.toString(retryAfterSeconds)),
                Optional.of(within(5, SECONDS, expectedRetryAfter)));

        // Verify that only one attempt was made
        assertThat(requests, is(1));
    }

    @Test
    public void failOn429_TimeLimitExceeded_Date()
            throws Exception
    {
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(1000)
                .setRetryLimit(3)
                .build()
                .httpClient;

        // A late Retry-After value to verify that the exception is propagated without any retries when
        // the Retry-After value exceeds the configured retryLimit * retryMaxInterval
        DateTime retryAfter = new DateTime().plusSeconds(4711);
        DateTimeFormatter httpDateFormatter = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss zzz");
        String retryAfterString = retryAfter.toString(httpDateFormatter);

        int requests = failWith429(
                Optional.of(retryAfterString),
                Optional.of(DateMatchers.within(30, SECONDS, retryAfter.toDate())));

        // Verify that only one attempt was made
        assertThat(requests, is(1));
    }

    @Test
    public void failOn429_RetryLimitExceededUsingExponentialBackOff()
            throws Exception
    {
        client = TDClient.newBuilder()
                .setRetryMaxIntervalMillis(Integer.MAX_VALUE)
                .setRetryStrategy(BackOffStrategy.Exponential)
                .setRetryLimit(3)
                .build()
                .httpClient;

        long retryAfterSeconds = 1;

        Date expectedRetryAfter = new Date(System.currentTimeMillis() + SECONDS.toMillis(retryAfterSeconds));

        int requests = failWith429(
                Optional.of(Long.toString(retryAfterSeconds)),
                Optional.of(within(5, SECONDS, expectedRetryAfter)));

        // Verify that 4 attempts were made (original request + three retries)
        assertThat(requests, is(4));
    }

    @Test
    public void readBodyAsBytes()
            throws Exception
    {
        final TDApiRequest req = TDApiRequest.Builder.GET("/v3/system/server_status").build();
        final byte[] body = new byte[3 * 1024 * 1024];
        Arrays.fill(body, (byte) 100);

        byte[] res = client.submitRequest(req, Optional.empty(), new TestDefaultHandler(body));
        assertThat(res, is(body));
    }

    private static class TestDefaultHandler
            implements TDHttpRequestHandler<byte[]>
    {
        private final byte[] body;

        public TestDefaultHandler(byte[] body)
        {
            this.body = body;
        }

        @Override
        public Response send(OkHttpClient httpClient, Request request)
                throws IOException
        {
            // Return a dummy response
            return new Response.Builder()
                    .request(request)
                    .code(200)
                    .message("")
                    .protocol(Protocol.HTTP_1_1)
                    .header("Content-Length", String.valueOf(body.length))
                    .body(ResponseBody.create(MediaType.parse("plain/text"), body))
                    .build();
        }

        @Override
        public byte[] onSuccess(Response response)
                throws Exception
        {
            return response.body().bytes();
        }
    }

    private int failWith429(final Optional<String> retryAfterValue, final Optional<Matcher<Date>> retryAfterMatcher)
    {
        final AtomicInteger requests = new AtomicInteger();

        final TDApiRequest req = TDApiRequest.Builder.GET("/v3/system/server_status").build();

        try {
            client.submitRequest(req, Optional.empty(), new TDHttpRequestHandler<byte[]>()
            {
                @Override
                public Response send(OkHttpClient httpClient, Request request)
                        throws IOException
                {
                    requests.incrementAndGet();
                    Response.Builder builder = new Response.Builder()
                            .request(request)
                            .protocol(Protocol.HTTP_1_1)
                            .message("")
                            .body(ResponseBody.create(null, ""))
                            .code(429);

                    if (retryAfterValue.isPresent()) {
                        builder = builder.header("Retry-After", retryAfterValue.get());
                    }
                    return builder.build();
                }

                @Override
                public byte[] onSuccess(Response response)
                        throws Exception
                {
                    return response.body().bytes();
                }
            });

            fail();
        }
        catch (TDClientException e) {
            if (!(e instanceof TDClientHttpTooManyRequestsException)) {
                fail("Expected " + TDClientHttpTooManyRequestsException.class + ", got " + e.getClass() + ": " + e.getMessage());
            }
            TDClientHttpTooManyRequestsException tooManyRequestsException = (TDClientHttpTooManyRequestsException) e;
            if (retryAfterMatcher.isPresent()) {
                assertThat(tooManyRequestsException.getRetryAfter().orElse(null), retryAfterMatcher.get());
            }
        }

        return requests.get();
    }
}
