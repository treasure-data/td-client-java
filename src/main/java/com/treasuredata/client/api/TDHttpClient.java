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
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import static com.treasuredata.client.TDClientException.ErrorType.*;
import com.treasuredata.client.ExponentialBackOffRetry;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientExecutionException;
import com.treasuredata.client.TDClientTimeoutException;
import com.treasuredata.client.TDClientHttpException;
import com.treasuredata.client.TDClientInterruptedException;
import com.treasuredata.client.api.model.TDApiError;
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

        this.objectMapper = new ObjectMapper()
                .registerModule(new JsonOrgModule()) // for mapping query json strings into JSONObject
                .registerModule(new GuavaModule())   // for mapping to Guava Optional class
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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

    protected Optional<TDApiError> parseErrorResponse(ContentResponse response) {
        try {
            return Optional.of(objectMapper.readValue(response.getContent(), TDApiError.class));
        }
        catch (IOException e) {
            logger.warn(String.format("Failed to parse error response: %s", response.getContentAsString()), e);
            return Optional.absent();
        }
    }

    public ContentResponse submit(TDApiRequest apiRequest)
            throws TDClientException
    {
        ExponentialBackOffRetry retry = new ExponentialBackOffRetry(config.getRetryLimit(), config.getRetryInitialWaitMillis(), config.getRetryWaitMillis());
        Optional<TDClientException> rootCause = Optional.absent();
        try {
            Optional<Integer> nextInterval = Optional.absent();
            do {
                if (retry.getExecutionCount() > 0) {
                    int waitTimeMillis = nextInterval.get();
                    logger.warn(String.format("Retrying request to %s (%d/%d) in %.2f sec.", apiRequest.getPath(), retry.getExecutionCount(), retry.getMaxRetryCount(), waitTimeMillis / 1000.0));
                    Thread.sleep(waitTimeMillis);
                }

                try {
                    Request request = apiRequest.newJettyRequest(httpClient, config);
                    logger.debug("Sending API request to {}", request.getURI());
                    ContentResponse response = request.send();
                    if (logger.isTraceEnabled()) {
                        logger.trace("response:\n" + response.getContentAsString());
                    }
                    int code = response.getStatus();
                    if (HttpStatus.isSuccess(code)) {
                        // 2xx success
                        logger.debug("[{}:{}] API request to %s has succeeded", code, response.getReason(), request.getPath());
                        return response;
                    }
                    else if (HttpStatus.isClientError(code)) {
                        // 4xx errors
                        logger.warn(String.format("[%d:%s] API request to %s has failed: %s", code, response.getReason(), request.getPath(), response.getContentAsString()));
                        TDClientException.ErrorType errorType = CLIENT_ERROR;
                        switch(code) {
                            case HttpStatus.UNAUTHORIZED_401:
                                errorType = AUTHENTICATION_FAILURE;
                                break;
                            case HttpStatus.NOT_FOUND_404:
                                errorType = DATABASE_OR_TABLE_NOT_FOUND;
                                break;
                            case HttpStatus.CONFLICT_409:
                                errorType = DATABASE_OR_TABLE_ALREADY_EXISTS;
                                break;
                        }
                        Optional<TDApiError> errorResponse = parseErrorResponse(response);
                        throw new TDClientHttpException(errorType, errorResponse.isPresent() ? errorResponse.get().toString() : response.getReason(), code);
                    }
                    else if (HttpStatus.isServerError(code)) {
                        // 5xx errors
                        String errorMessage = String.format("[%d:%s] API request to %s has failed", code, response.getReason(), request.getPath());
                        logger.warn(errorMessage);
                        rootCause = Optional.<TDClientException>of(new TDClientHttpException(SERVER_ERROR, errorMessage, code));
                    }
                    else {
                        String errorMessage = String.format("[%d:%s] API request to %s has failed", code, response.getReason(), request.getPath());
                        logger.warn(errorMessage);
                        rootCause = Optional.<TDClientException>of(new TDClientHttpException(UNEXPECTED_RESPONSE_CODE, errorMessage, code));
                    }
                }
                catch (ExecutionException e) {
                    rootCause = Optional.<TDClientException>of(new TDClientExecutionException(e));
                    logger.warn("API request failed", e);
                }
                catch (TimeoutException e) {
                    rootCause = Optional.<TDClientException>of(new TDClientTimeoutException(e));
                    logger.warn(String.format("API request to %s has timed out", apiRequest.getPath()), e);
                }
            }
            while ((nextInterval = retry.nextWaitTimeMillis()).isPresent());
        }
        catch (InterruptedException e) {
            throw new TDClientInterruptedException(e);
        }

        if(rootCause.isPresent()) {
            // Throw the last seen error
            throw rootCause.get();
        }
        else {
            throw new TDClientException(RETRY_LIMIT_EXCEEDED, String.format("Failed to process the API request to %s", apiRequest.getPath()));
        }
    }

    public <Result> Result submit(TDApiRequest request, Class<Result> resultType)
            throws TDClientException
    {
        try {
            ContentResponse response = submit(request);
            return objectMapper.readValue(response.getContent(), resultType);
        }
        catch (JsonMappingException e) {
            logger.error("Jackson mapping error", e);
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
        catch (IOException e) {
            throw new TDClientException(RESPONSE_READ_FAILURE, e);
        }
    }
}
