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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.treasuredata.client.model.TDApiError;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static com.treasuredata.client.TDClientException.ErrorType.CLIENT_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;
import static com.treasuredata.client.TDClientException.ErrorType.RESPONSE_READ_FAILURE;
import static com.treasuredata.client.TDClientException.ErrorType.SERVER_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.UNEXPECTED_RESPONSE_CODE;
import static org.msgpack.core.Preconditions.checkNotNull;

/**
 * An extension of Jetty HttpClient with request retry handler
 */
public class TDHttpClient
{
    private static final Logger logger = LoggerFactory.getLogger(TDHttpClient.class);
    private final TDClientConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private Optional<String> credentialCache = Optional.absent();

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

    protected Optional<TDApiError> parseErrorResponse(byte[] content)
    {
        try {
            return Optional.of(objectMapper.readValue(content, TDApiError.class));
        }
        catch (IOException e) {
            logger.warn(String.format("Failed to parse error response: %s", new String(content)), e);
            return Optional.absent();
        }
    }

    public static interface Handler<ResponseType extends Response, Result> {
        ResponseType submit(Request requset)
                throws InterruptedException, ExecutionException, TimeoutException;

        Result onSuccess(ResponseType response);

        /**
         *
         * @param response
         * @return error message
         */
        String onError(ResponseType response);
    }


    public class ContentStreamHandler implements Handler<Response, InputStream> {
        private InputStreamResponseListener listener = null;

        @Override
        public Response submit(Request request)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            listener = new InputStreamResponseListener();
            request.send(listener);
            long timeout = httpClient.getIdleTimeout();
            return listener.get(timeout, TimeUnit.MILLISECONDS);
        }

        @Override
        public InputStream onSuccess(Response response)
        {
            checkNotNull(listener, "listener is null");
            return listener.getInputStream();
        }

        @Override
        public String onError(Response response)
        {
            int code = response.getStatus();
            InputStream in = null;
            try {
                try {
                    in = listener.getInputStream();
                    byte[] content = ByteStreams.toByteArray(in);
                    return reportErrorMessage(response, content);
                }
                finally {
                    if(in != null)  {
                        in.close();
                    }
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public class ContentHandler implements Handler<ContentResponse, ContentResponse> {
        @Override
        public ContentResponse submit(Request request)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            return request.send();
        }

        @Override
        public ContentResponse onSuccess(ContentResponse response)
        {
            if (logger.isTraceEnabled()) {
                logger.trace("response:\n" + response.getContentAsString());
            }
            return response;
        }

        @Override
        public String onError(ContentResponse response)
        {
            return reportErrorMessage(response, response.getContent());
        }
    }

    protected String reportErrorMessage(Response response, byte[] responseContentOnError) {
        int code = response.getStatus();
        Optional<TDApiError> errorResponse = parseErrorResponse(responseContentOnError);
        String responseErrorText = errorResponse.isPresent() ? ": " + errorResponse.get().getText() : "";
        String errorMessage = String.format("[%d:%s] API request to %s has failed%s", code, response.getReason(), response.getRequest().getPath(), responseErrorText);
        return errorMessage;
    }

    public <ResponseType extends Response, Result> Result submit(TDApiRequest apiRequest, Handler<ResponseType, Result> requestHandler)
            throws TDClientException
    {
        ExponentialBackOffRetry retry = new ExponentialBackOffRetry(config.getRetryLimit(), config.getRetryInitialWaitMillis(), config.getRetryIntervalMillis());
        Optional<TDClientException> rootCause = Optional.absent();
        Request request = null;
        try {
            Optional<Integer> nextInterval = Optional.absent();
            do {
                if (retry.getExecutionCount() > 0) {
                    int waitTimeMillis = nextInterval.get();
                    logger.warn(String.format("Retrying request to %s (%d/%d) in %.2f sec.", apiRequest.getPath(), retry.getExecutionCount(), retry.getMaxRetryCount(), waitTimeMillis / 1000.0));
                    Thread.sleep(waitTimeMillis);
                }

                request = apiRequest.newJettyRequest(httpClient, config, credentialCache);
                try {
                    logger.debug("Sending API request to {}", request.getURI());
                    ResponseType response = requestHandler.submit(request);
                    int code = response.getStatus();
                    if (HttpStatus.isSuccess(code)) {
                        // 2xx success
                        logger.info(String.format("[%d:%s] API request to %s has succeeded", code, response.getReason(), request.getPath()));
                        return requestHandler.onSuccess(response);
                    }
                    else {
                        String errorMessage = requestHandler.onError(response);
                        if (HttpStatus.isClientError(code)) {
                            logger.error(errorMessage);
                            // 4xx error. We do not retry the execution on this type of error
                            switch (code) {
                                case HttpStatus.UNAUTHORIZED_401:
                                    throw new TDClientHttpUnauthorizedException(errorMessage);
                                case HttpStatus.NOT_FOUND_404:
                                    throw new TDClientHttpNotFoundException(errorMessage);
                                case HttpStatus.CONFLICT_409:
                                    throw new TDClientHttpConflictException(errorMessage);
                                default:
                                    throw new TDClientHttpException(CLIENT_ERROR, errorMessage, code);
                            }
                        }
                        logger.warn(errorMessage);
                        if (HttpStatus.isServerError(code)) {
                            // 5xx errors
                            rootCause = Optional.<TDClientException>of(new TDClientHttpException(SERVER_ERROR, errorMessage, code));
                        }
                        else {
                            rootCause = Optional.<TDClientException>of(new TDClientHttpException(UNEXPECTED_RESPONSE_CODE, errorMessage, code));
                        }
                    }
                }
                catch (ExecutionException e) {
                    logger.warn("API request failed", e);
                    rootCause = Optional.<TDClientException>of(new TDClientExecutionException(e));
                }
                catch (TimeoutException e) {
                    logger.warn(String.format("API request to %s has timed out", apiRequest.getPath()), e);
                    rootCause = Optional.<TDClientException>of(new TDClientTimeoutException(e));
                    request.abort(e);
                }
            }
            while ((nextInterval = retry.nextWaitTimeMillis()).isPresent());
        }
        catch (InterruptedException e) {
            logger.warn("API request interrupted", e);
            if(request != null) {
                request.abort(e);
            }
            throw new TDClientInterruptedException(e);
        }
        logger.warn("API request retry limit exceeded: ({}/{})", config.getRetryLimit(), config.getRetryLimit());

        checkState(rootCause.isPresent(), "rootCause must be present here");
        // Throw the last seen error
        throw rootCause.get();
    }

    public ContentResponse submit(TDApiRequest apiRequest) {
        return submit(apiRequest, new ContentHandler());
    }

    public InputStream openStream(TDApiRequest apiRequest) {
        return submit(apiRequest, new ContentStreamHandler());
    }

    public <Result> Result submit(TDApiRequest apiRequest, Class<Result> resultType)
            throws TDClientException
    {
        try {
            ContentResponse response = submit(apiRequest);
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


    public void setCredentialCache(String apikey) {
        this.credentialCache = Optional.of(apikey);
    }

}
