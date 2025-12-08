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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * JDK HTTP Server-based replacement for OkHttp MockWebServer
 * Provides compatibility for existing test code during JDK 17 migration
 */
public class MockHttpServer implements AutoCloseable
{
    private HttpServer httpServer;
    private final BlockingQueue<MockResponse> responses = new LinkedBlockingQueue<>();
    private final List<RecordedRequest> recordedRequests = new ArrayList<>();
    private String hostname = "localhost";
    private int port;

    public static class MockResponse
    {
        private String body = "";
        private int responseCode = 200;
        private final Map<String, String> headers = new java.util.HashMap<>();

        public MockResponse setBody(String body)
        {
            this.body = body;
            return this;
        }

        public MockResponse setResponseCode(int code)
        {
            this.responseCode = code;
            return this;
        }

        public MockResponse setHeader(String name, String value)
        {
            this.headers.put(name, value);
            return this;
        }

        public String getBody()
        {
            return body;
        }

        public int getResponseCode()
        {
            return responseCode;
        }

        public Map<String, String> getHeaders()
        {
            return headers;
        }
    }

    public static class RecordedRequest
    {
        private final String method;
        private final URI uri;
        private final String path;
        private final Map<String, List<String>> headers;
        private final String body;

        public RecordedRequest(String method, URI uri, String path, Map<String, List<String>> headers, String body)
        {
            this.method = method;
            this.uri = uri;
            this.path = path;
            this.headers = headers;
            this.body = body;
        }

        public String getMethod()
        {
            return method;
        }

        public URI getUri()
        {
            return uri;
        }

        public String getPath()
        {
            return path;
        }

        public String getHeader(String name)
        {
            List<String> values = headers.get(name);
            return values != null && !values.isEmpty() ? values.get(0) : null;
        }

        public String getBody()
        {
            return body;
        }
    }

    public void enqueue(MockResponse response)
    {
        responses.offer(response);
    }

    public void start(int port) throws IOException
    {
        this.port = port;
        httpServer = HttpServer.create(new InetSocketAddress(hostname, port), 0);
        httpServer.createContext("/", new RequestHandler());
        httpServer.setExecutor(null); // Use default executor
        httpServer.start();
    }

    public void start() throws IOException
    {
        start(0); // Use any available port
    }

    public String getHostName()
    {
        return hostname;
    }

    public int getPort()
    {
        return httpServer != null ? httpServer.getAddress().getPort() : port;
    }

    public int getRequestCount()
    {
        return recordedRequests.size();
    }

    public RecordedRequest takeRequest() throws InterruptedException
    {
        // Return the most recent request for simplicity
        if (!recordedRequests.isEmpty()) {
            return recordedRequests.get(recordedRequests.size() - 1);
        }
        return null;
    }

    @Override
    public void close()
    {
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
        responses.clear();
        recordedRequests.clear();
    }

    private class RequestHandler implements HttpHandler
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            try {
                // Read request body
                String requestBody = "";
                try (InputStream is = exchange.getRequestBody()) {
                    requestBody = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                }

                // Record the request
                RecordedRequest recordedRequest = new RecordedRequest(
                        exchange.getRequestMethod(),
                        exchange.getRequestURI(),
                        exchange.getRequestURI().getPath() +
                                (exchange.getRequestURI().getQuery() != null ? "?" + exchange.getRequestURI().getQuery() : ""),
                        exchange.getRequestHeaders(),
                        requestBody
                );
                synchronized (recordedRequests) {
                    recordedRequests.add(recordedRequest);
                }

                // Get next response or use default
                MockResponse response = responses.poll();
                if (response == null) {
                    response = new MockResponse(); // Default 200 OK with empty body
                }

                // Set response headers
                for (Map.Entry<String, String> header : response.getHeaders().entrySet()) {
                    exchange.getResponseHeaders().set(header.getKey(), header.getValue());
                }

                // Send response
                byte[] responseBytes = response.getBody().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(response.getResponseCode(), responseBytes.length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBytes);
                }
            }
            catch (Exception e) {
                // Send error response
                byte[] errorBytes = "Internal Server Error".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, errorBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorBytes);
                }
            }
        }
    }
}