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
package com.treasuredata.client.api;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.util.HttpCookieStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class JettyHttpClient
{
    private static final Logger logger = LoggerFactory.getLogger(JettyHttpClient.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public JettyHttpClient() {

        this.objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        this.httpClient = new HttpClient();
        httpClient.setConnectTimeout(10 * 1000);
        httpClient.setIdleTimeout(60 * 1000);
        httpClient.setMaxConnectionsPerDestination(16);
        httpClient.setCookieStore(new HttpCookieStore.Empty());

        try {
            httpClient.start();
        }
        catch (Exception e) {
            logger.error("Failed to initialize Jetty client", e);
            throw Throwables.propagate(e);
        }
    }

    public void close() {
        try {
            httpClient.stop();
        }
        catch (Exception e) {
            logger.error("Failed to terminate Jetty client", e);
            throw Throwables.propagate(e);
        }
    }

    public Request newRequest(String uri) {
        return httpClient.newRequest(uri);
    }


    public Response submit(Request request)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        return request.send();
    }

}
