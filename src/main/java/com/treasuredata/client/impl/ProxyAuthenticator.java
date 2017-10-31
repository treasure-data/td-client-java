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
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class ProxyAuthenticator
        implements Authenticator
{
    private final Logger logger = LoggerFactory.getLogger(ProxyAuthenticator.class);
    private final ProxyConfig proxyConfig;
    private Optional<String> proxyAuthCache = Optional.absent();

    public ProxyAuthenticator(ProxyConfig proxyConfig)
    {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public Request authenticate(Route route, Response response)
            throws IOException
    {
        logger.debug("Proxy authorization requested for " + route.address());
        if (!proxyAuthCache.isPresent()) {
            proxyAuthCache = Optional.of(
                    Credentials.basic(proxyConfig.getUser().or(""), proxyConfig.getPassword().or(""))
            );
        }
        return response.request().newBuilder()
                .header("Proxy-Authrization", proxyAuthCache.get())
                .build();
    }
}
