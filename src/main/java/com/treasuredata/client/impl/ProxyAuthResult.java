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
package com.treasuredata.client.impl;

import com.google.common.base.Optional;
import com.treasuredata.client.ProxyConfig;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.B64Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class ProxyAuthResult
        implements Authentication.Result
{
    private final Logger logger = LoggerFactory.getLogger(ProxyAuthResult.class);
    private final ProxyConfig proxyConfig;
    private Optional<String> proxyAuthCache = Optional.absent();

    public ProxyAuthResult(ProxyConfig proxyConfig)
    {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public URI getURI()
    {
        return proxyConfig.getUri();
    }

    @Override
    public void apply(Request request)
    {
        logger.debug("Proxy authorization requested for " + request.getPath());
        if (!proxyAuthCache.isPresent()) {
            proxyAuthCache = Optional.of("Basic " + B64Code.encode(proxyConfig.getUser().or("") + ":" + proxyConfig.getPassword().or(""), StandardCharsets.ISO_8859_1));
        }
        request.header(HttpHeader.PROXY_AUTHORIZATION, proxyAuthCache.get());
    }
}
