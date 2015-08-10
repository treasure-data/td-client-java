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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.treasuredata.client.ErrorCode;
import com.treasuredata.client.ExponentialBackOffRetry;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.TDClientException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * An extension of Jetty HttpClient with request retry handler
 */
public class TDHttpClient
{
    private static final Logger logger = LoggerFactory.getLogger(TDHttpClient.class);
    private final TDClientConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public TDHttpClient(TDClientConfig config)
    {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        this.httpClient = new HttpClient();
        httpClient.setConnectTimeout(3000);
        httpClient.setAddressResolutionTimeout(3000);
        httpClient.setIdleTimeout(10000);
        httpClient.setTCPNoDelay(true);
        httpClient.setExecutor(new QueuedThreadPool());
        httpClient.setMaxConnectionsPerDestination(64);
        httpClient.setCookieStore(new HttpCookieStore.Empty());

        try {
            httpClient.start();
        }
        catch (Exception e) {
            logger.error("Failed to initialize Jetty client", e);
            throw Throwables.propagate(e);
        }
    }

    public void close()
    {
        try {
            httpClient.stop();
        }
        catch (Exception e) {
            logger.error("Failed to terminate Jetty client", e);
            throw Throwables.propagate(e);
        }
    }

    public ContentResponse submit(ApiRequest apiRequest)
            throws TDClientException
    {
        ExponentialBackOffRetry retry = new ExponentialBackOffRetry(config.getRetryLimit(), config.getRetryInitialWaitMillis(), config.getRetryWaitMillis());
        Optional<Exception> rootCause = Optional.absent();
        try {
            while (retry.isRunnable()) {
                try {
                    Request request = apiRequest.newJettyRequest(httpClient, config);
                    ContentResponse response = request.send();
                    int code = response.getStatus();
                    if (HttpStatus.isSuccess(code)) {
                        // 2xx success
                        return response;
                    }
                    else if (HttpStatus.isClientError(code)) {
                        // 4xx errors
                        throw new TDClientException(ErrorCode.API_CLIENT_ERROR, response.getReason());
                    }
                    else if (HttpStatus.isServerError(code)) {
                        // 5xx errors
                        logger.warn("API request to %s failed with %d: %s", request.getPath(), code, response.getReason());
                    }
                    else {
                        logger.warn("API request to %s failed with code %d: %s", request.getPath(), code, response.getReason());
                    }
                }
                catch (ExecutionException e) {
                    rootCause = Optional.<Exception>of(e);
                    logger.warn("API request failed", e);
                }
                catch (TimeoutException e) {
                    rootCause = Optional.<Exception>of(e);
                    logger.warn(String.format("API request to %s timed out", apiRequest.getUri()), e);
                }
                int waitTimeMs = retry.nextWaitTimeMillis();
                Thread.sleep(waitTimeMs);
                logger.warn(String.format("Retrying request to %s (%d/%d) %,d", apiRequest.getUri(), retry.getRetryCount(), retry.getMaxRetryCount(), waitTimeMs));
            }
        }
        catch (InterruptedException e) {
            throw new TDClientException(ErrorCode.API_EXECUTION_INTERRUPTED, e);
        }
        throw new TDClientException(ErrorCode.API_RETRY_LIMIT_EXCEEDED, String.format("Failed to process API request to %s", apiRequest.getUri()), rootCause);
    }

    public <Result> Result submit(ApiRequest request, Class<Result> resultType)
            throws TDClientException
    {
        try {
            ContentResponse response = submit(request);
            if (logger.isTraceEnabled()) {
                logger.trace("response json:\n" + response.getContentAsString());
            }
            return objectMapper.readValue(response.getContent(), resultType);
        }
        catch (JsonMappingException e) {
            logger.error("Jackson mapping error", e);
            throw new TDClientException(ErrorCode.API_INVALID_JSON_RESPONSE, e);
        }
        catch (IOException e) {
            throw new TDClientException(ErrorCode.API_INVALID_JSON_RESPONSE, e);
        }
    }
}
