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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.treasuredata.client.impl.ProxyAuthResult;
import com.treasuredata.client.model.TDApiErrorMessage;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.treasuredata.client.TDApiRequest.urlEncode;
import static com.treasuredata.client.TDClientException.ErrorType.CLIENT_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;
import static com.treasuredata.client.TDClientException.ErrorType.PROXY_AUTHENTICATION_FAILURE;
import static com.treasuredata.client.TDClientException.ErrorType.SERVER_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.UNEXPECTED_RESPONSE_CODE;

/**
 * An extension of Jetty HttpClient with request retry handler
 */
public class TDHttpClient
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(TDHttpClient.class);
    protected final TDClientConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public TDHttpClient(TDClientConfig config)
    {
        this.config = config;
        this.httpClient = config.useSSL ? new HttpClient(new SslContextFactory()) : new HttpClient();
        httpClient.setConnectTimeout(config.connectTimeoutMillis);
        httpClient.setIdleTimeout(config.idleTimeoutMillis);
        httpClient.setTCPNoDelay(true);
        httpClient.setExecutor(new QueuedThreadPool(config.connectionPoolSize, 2));
        httpClient.setCookieStore(new HttpCookieStore.Empty());
        httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, "td-client-java-" + TDClient.getVersion()));

        // Proxy configuration
        if (config.proxy.isPresent()) {
            final ProxyConfig proxyConfig = config.proxy.get();
            logger.trace("proxy configuration: " + proxyConfig);
            HttpProxy httpProxy = new HttpProxy(new Origin.Address(proxyConfig.getHost(), proxyConfig.getPort()), proxyConfig.useSSL());

            // Do not proxy requests for the proxy server
            httpProxy.getExcludedAddresses().add(proxyConfig.getHost() + ":" + proxyConfig.getPort());
            httpClient.getProxyConfiguration().getProxies().add(httpProxy);
            if (proxyConfig.requireAuthentication()) {
                httpClient.getAuthenticationStore().addAuthenticationResult(new ProxyAuthResult(proxyConfig));
            }
        }
        // Prepare jackson json-object mapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JsonOrgModule()) // for mapping query json strings into JSONObject
                .registerModule(new GuavaModule())   // for mapping to Guava Optional class
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
        synchronized (this) {
            try {
                httpClient.stop();
            }
            catch (Exception e) {
                logger.error("Failed to terminate Jetty client", e);
                throw Throwables.propagate(e);
            }
        }
    }

    @VisibleForTesting
    Optional<TDApiErrorMessage> parseErrorResponse(byte[] content)
    {
        try {
            if (content.length > 0 && content[0] == '{') {
                // Error message from TD API
                return Optional.of(objectMapper.readValue(content, TDApiErrorMessage.class));
            }
            else {
                // Error message from Proxy server etc.
                String contentStr = new String(content, StandardCharsets.UTF_8);
                return Optional.of(new TDApiErrorMessage("error", contentStr, "error"));
            }
        }
        catch (IOException e) {
            logger.warn(String.format("Failed to parse error response: %s", new String(content, StandardCharsets.UTF_8)), e);
            return Optional.absent();
        }
    }

    private static final ThreadLocal<SimpleDateFormat> RFC2822_FORMAT =
            new ThreadLocal<SimpleDateFormat>()
            {
                @Override
                protected SimpleDateFormat initialValue()
                {
                    return new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH);
                }
            };

    protected Request setTDAuthHeaders(Request request, String dateHeader)
    {
        // Do nothing
        return request;
    }

    protected String getClientName()
    {
        return "td-client-java " + TDClient.getVersion();
    }

    public Request prepareRequest(TDApiRequest apiRequest, Optional<String> apiKeyCache)
    {
        String queryStr = "";
        String portStr = config.port.transform(new Function<Integer, String>()
        {
            @Override
            public String apply(Integer input)
            {
                return ":" + input.toString();
            }
        }).or("");
        String requestUri = String.format("%s://%s%s%s", config.useSSL ? "https" : "http", config.endpoint, portStr, apiRequest.getPath());

        if (!apiRequest.getQueryParams().isEmpty()) {
            List<String> queryParamList = new ArrayList<String>(apiRequest.getQueryParams().size());
            for (Map.Entry<String, String> queryParam : apiRequest.getQueryParams().entrySet()) {
                queryParamList.add(String.format("%s=%s", urlEncode(queryParam.getKey()), urlEncode(queryParam.getValue())));
            }
            queryStr = Joiner.on("&").join(queryParamList);
            if (apiRequest.getMethod() == HttpMethod.GET ||
                    (apiRequest.getMethod() == HttpMethod.POST && apiRequest.getPostJson().isPresent())) {
                requestUri += "?" + queryStr;
            }
        }

        logger.debug("Sending API request to {}", requestUri);
        String dateHeader = RFC2822_FORMAT.get().format(new Date());
        Request request = httpClient.newRequest(requestUri)
                .agent(getClientName())
                .scheme(config.useSSL ? "https" : "http")
                .method(apiRequest.getMethod())
                .header(HttpHeader.DATE, dateHeader);

        request = setTDAuthHeaders(request, dateHeader);

        // Set API Key
        Optional<String> apiKey = apiKeyCache.or(config.apiKey);
        if (apiKey.isPresent()) {
            request.header(HttpHeader.AUTHORIZATION, "TD1 " + apiKey.get());
        }

        // Set other headers
        for (Map.Entry<String, String> entry : apiRequest.getHeaderParams().entrySet()) {
            request.header(entry.getKey(), entry.getValue());
        }

        // Submit method specific headers
        switch (apiRequest.getMethod()) {
            case POST:
                if (apiRequest.getPostJson().isPresent()) {
                    request.content(new StringContentProvider(apiRequest.getPostJson().get()), "application/json");
                }
                else if (queryStr.length() > 0) {
                    request.content(new StringContentProvider(queryStr), "application/x-www-form-urlencoded");
                }
                else {
                    // We should set content-length explicitly for an empty post
                    request.header("Content-Length", "0");
                }
                break;
            case PUT:
                if (apiRequest.getPutFile().isPresent()) {
                    try {
                        request.file(apiRequest.getPutFile().get().toPath(), "application/octet-stream");
                    }
                    catch (IOException e) {
                        throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, "Failed to read input file: " + apiRequest.getPutFile().get());
                    }
                }
                break;
        }
        return request;
    }

    public <ResponseType extends Response, Result> Result submitRequest(TDApiRequest apiRequest, Optional<String> apiKeyCache, Handler<ResponseType, Result> handler)
            throws TDClientException
    {
        ExponentialBackOff backoff = new ExponentialBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier);
        Optional<TDClientException> rootCause = Optional.absent();
        try {
            final int retryLimit = config.retryLimit;
            for (int retryCount = 0; retryCount <= retryLimit; ++retryCount) {
                if (retryCount > 0) {
                    int waitTimeMillis = backoff.nextWaitTimeMillis();
                    logger.warn(String.format("Retrying request to %s (%d/%d) in %.2f sec.", apiRequest.getPath(), backoff.getExecutionCount(), retryLimit, waitTimeMillis / 1000.0));
                    Thread.sleep(waitTimeMillis);
                }

                ResponseType response = null;
                try {
                    Request request = prepareRequest(apiRequest, apiKeyCache);
                    response = handler.submit(request);
                    int code = response.getStatus();
                    if (HttpStatus.isSuccess(code)) {
                        // 2xx success
                        logger.info(String.format("[%d:%s] API request to %s has succeeded", code, HttpStatus.getMessage(code), apiRequest.getPath()));
                        return handler.onSuccess(response);
                    }
                    else {
                        byte[] returnedContent = handler.onError(response);
                        rootCause = Optional.of(handleHttpResponseError(apiRequest.getPath(), code, returnedContent));
                    }
                }
                catch (ExecutionException e) {
                    logger.warn("API request failed", e);
                    // Jetty client + jersey may return ProcessingException for 401 errors
                    Optional<HttpResponseException> responseError = findHttpResponseException(e);
                    if (responseError.isPresent()) {
                        HttpResponseException re = responseError.get();
                        int code = re.getResponse().getStatus();
                        throw handleHttpResponseError(apiRequest.getPath(), code, new byte[] {});
                    }
                    else {
                        throw new TDClientProcessingException(e);
                    }
                }
                catch (TimeoutException e) {
                    logger.warn(String.format("API request to %s has timed out", apiRequest.getPath()), e);
                    rootCause = Optional.<TDClientException>of(new TDClientTimeoutException(e));
                }
            }
        }
        catch (InterruptedException e) {
            logger.warn("API request interrupted", e);
            throw new TDClientInterruptedException(e);
        }
        logger.warn("API request retry limit exceeded: ({}/{})", config.retryLimit, config.retryLimit);

        checkState(rootCause.isPresent(), "rootCause must be present here");
        // Throw the last seen error
        throw rootCause.get();
    }

    protected TDClientException handleHttpResponseError(String apiRequestPath, int code, byte[] returnedContent)
    {
        Optional<TDApiErrorMessage> errorResponse = parseErrorResponse(returnedContent);
        String responseErrorText = errorResponse.isPresent() ? ": " + errorResponse.get().getText() : "";
        String errorMessage = String.format("[%d:%s] API request to %s has failed%s", code, HttpStatus.getMessage(code), apiRequestPath, responseErrorText);
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
                case HttpStatus.PROXY_AUTHENTICATION_REQUIRED_407:
                    throw new TDClientHttpException(PROXY_AUTHENTICATION_FAILURE, errorMessage, code);
                default:
                    throw new TDClientHttpException(CLIENT_ERROR, errorMessage, code);
            }
        }
        logger.warn(errorMessage);
        if (HttpStatus.isServerError(code)) {
            // 5xx errors
            return new TDClientHttpException(SERVER_ERROR, errorMessage, code);
        }
        else {
            throw new TDClientHttpException(UNEXPECTED_RESPONSE_CODE, errorMessage, code);
        }
    }

    protected static Optional<HttpResponseException> findHttpResponseException(Throwable e)
    {
        if (e == null) {
            return Optional.absent();
        }
        else {
            if (HttpResponseException.class.isAssignableFrom(e.getClass())) {
                return Optional.of((HttpResponseException) e);
            }
            else {
                return findHttpResponseException(e.getCause());
            }
        }
    }

    public String call(TDApiRequest apiRequest, Optional<String> apiKeyCache)
    {
        ContentResponse response = submitRequest(apiRequest, apiKeyCache, new DefaultContentHandler());
        String content = response.getContentAsString();
        if (logger.isTraceEnabled()) {
            logger.trace("response:\n{}", content);
        }
        return content;
    }

    public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final Function<InputStream, Result> contentStreamHandler)
    {
        InputStream input = submitRequest(apiRequest, apiKeyCache, new ContentStreamHandler());
        return contentStreamHandler.apply(input);
    }

    /**
     * Submit an API request, and bind the returned JSON data into an object of the given result type.
     * For mapping it uses Jackson object mapper.
     *
     * @param apiRequest
     * @param resultType
     * @param <Result>
     * @return
     * @throws TDClientException
     */
    public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final Class<Result> resultType)
            throws TDClientException
    {
        try {
            ContentResponse response = submitRequest(apiRequest, apiKeyCache, new DefaultContentHandler());
            byte[] content = response.getContent();
            if (logger.isTraceEnabled()) {
                logger.trace("response:\n{}", new String(content, StandardCharsets.UTF_8));
            }
            return objectMapper.readValue(content, resultType);
        }
        catch (JsonMappingException e) {
            logger.error("Jackson mapping error", e);
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
        catch (IOException e) {
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
    }

    public static interface Handler<ResponseType extends Response, Result>
    {
        ResponseType submit(Request request)
                throws InterruptedException, ExecutionException, TimeoutException;

        Result onSuccess(ResponseType response);

        /**
         * @param response
         * @return returned content
         */
        byte[] onError(ResponseType response);
    }

    class ContentStreamHandler
            implements Handler<Response, InputStream>
    {
        private InputStreamResponseListener listner = null;

        public Response submit(Request request)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            listner = new InputStreamResponseListener();
            request.send(listner);
            long timeout = httpClient.getIdleTimeout();
            return listner.get(timeout, TimeUnit.MILLISECONDS);
        }

        public InputStream onSuccess(Response response)
        {
            checkNotNull(listner, "listner is null");
            return listner.getInputStream();
        }

        public byte[] onError(Response response)
        {
            try (InputStream in = listner.getInputStream()) {
                byte[] errorResponse = ByteStreams.toByteArray(in);
                return errorResponse;
            }
            catch (IOException e) {
                throw new TDClientException(INVALID_JSON_RESPONSE, e);
            }
        }
    }

    public static class DefaultContentHandler
            implements Handler<ContentResponse, ContentResponse>
    {
        @Override
        public ContentResponse submit(Request request)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            return request.send();
        }

        @Override
        public ContentResponse onSuccess(ContentResponse response)
        {
            return response;
        }

        @Override
        public byte[] onError(ContentResponse response)
        {
            return response.getContent();
        }
    }
}
