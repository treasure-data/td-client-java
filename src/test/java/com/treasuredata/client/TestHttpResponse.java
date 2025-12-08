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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLSession;

/**
 * Test implementation of HttpResponse for unit testing
 */
public class TestHttpResponse implements HttpResponse<String>
{
    private final int statusCode;
    private final String body;
    private final Map<String, List<String>> headers;
    private final URI uri;

    public TestHttpResponse(int statusCode, String body, Map<String, List<String>> headers, URI uri)
    {
        this.statusCode = statusCode;
        this.body = body;
        this.headers = headers != null ? headers : new HashMap<>();
        this.uri = uri != null ? uri : URI.create("https://api.treasuredata.com/v3/server_status");
    }

    @Override
    public int statusCode()
    {
        return statusCode;
    }

    @Override
    public HttpRequest request()
    {
        return HttpRequest.newBuilder(uri).build();
    }

    @Override
    public Optional<HttpResponse<String>> previousResponse()
    {
        return Optional.empty();
    }

    @Override
    public HttpHeaders headers()
    {
        return HttpHeaders.of(headers, (s, s2) -> true);
    }

    @Override
    public String body()
    {
        return body;
    }

    @Override
    public Optional<SSLSession> sslSession()
    {
        return Optional.empty();
    }

    @Override
    public URI uri()
    {
        return uri;
    }

    @Override
    public HttpClient.Version version()
    {
        return HttpClient.Version.HTTP_1_1;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int statusCode = 200;
        private String body = "";
        private Map<String, List<String>> headers = new HashMap<>();
        private URI uri = URI.create("https://api.treasuredata.com/v3/server_status");

        public Builder statusCode(int statusCode)
        {
            this.statusCode = statusCode;
            return this;
        }

        public Builder body(String body)
        {
            this.body = body;
            return this;
        }

        public Builder header(String name, String value)
        {
            this.headers.put(name, List.of(value));
            return this;
        }

        public Builder uri(URI uri)
        {
            this.uri = uri;
            return this;
        }

        public TestHttpResponse build()
        {
            return new TestHttpResponse(statusCode, body, headers, uri);
        }
    }
}