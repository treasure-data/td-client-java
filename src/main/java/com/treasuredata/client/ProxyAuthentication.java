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

import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.Attributes;
import org.eclipse.jetty.util.B64Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class ProxyAuthentication
        implements Authentication
{
    private static final Logger logger = LoggerFactory.getLogger(ProxyAuthentication.class);
    private final String user;
    private final String password;

    public ProxyAuthentication(String password, String user)
    {
        this.password = password;
        this.user = user;
    }

    @Override
    public boolean matches(String type, URI uri, String realm)
    {
        if (!"basic".equalsIgnoreCase(type)) {
            return false;
        }
        return true;
    }

    @Override
    public Result authenticate(Request request, ContentResponse response, HeaderInfo headerInfo, Attributes context)
    {
        String value = "Basic " + B64Code.encode(user + ":" + password, StandardCharsets.ISO_8859_1);
        logger.debug("paroxy auth request: " + value);
        return new ProxyAuthResult(headerInfo.getHeader(), request.getURI(), value);
    }

    private static class ProxyAuthResult
            implements Result
    {
        private final HttpHeader header;
        private final URI uri;
        private final String value;

        public ProxyAuthResult(HttpHeader header, URI uri, String value)
        {
            this.header = header;
            this.uri = uri;
            this.value = value;
        }

        @Override
        public URI getURI()
        {
            return uri;
        }

        @Override
        public void apply(Request request)
        {
            request.header(header, value);
        }

        @Override
        public String toString()
        {
            return String.format("Basic authentication result for %s", uri);
        }
    }
}
