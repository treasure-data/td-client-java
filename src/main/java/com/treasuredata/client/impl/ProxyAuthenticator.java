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

import com.treasuredata.client.ProxyConfig;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpException;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.PROXY_AUTHORIZATION;

/**
 *
 */
public class ProxyAuthenticator
        implements Authenticator
{
    private final Logger logger = LoggerFactory.getLogger(ProxyAuthenticator.class);
    private final ProxyConfig proxyConfig;
    private Optional<String> proxyAuthCache = Optional.empty();

    public ProxyAuthenticator(ProxyConfig proxyConfig)
    {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public Request authenticate(Route route, Response response)
            throws IOException
    {
        if (response.request().header(PROXY_AUTHORIZATION) != null) {
            // If Proxy-Authrization is set, it means OkHttp client has already tried the authentication and failed
            throw new IOException(new TDClientHttpException(TDClientException.ErrorType.PROXY_AUTHENTICATION_FAILURE, "Proxy authentication failure", 407, null));
        }

        // Proxy authentication is required
        if (!proxyAuthCache.isPresent()) {
            logger.debug("Proxy authorization requested for " + route.address());
            proxyAuthCache = Optional.of(
                    Credentials.basic(proxyConfig.getUser().orElse(""), proxyConfig.getPassword().orElse(""))
            );
        }
        return response.request().newBuilder()
                .addHeader(PROXY_AUTHORIZATION, proxyAuthCache.get())
                .build();
    }
}
