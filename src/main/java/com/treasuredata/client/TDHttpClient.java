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

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.treasuredata.client.impl.ProxyAuthenticator;
import com.treasuredata.client.model.JsonCollectionRootName;
import com.treasuredata.client.model.TDApiErrorMessage;
import okhttp3.ConnectionPool;
import okhttp3.JavaNetCookieJar;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.ConnectException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.Proxy;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.treasuredata.client.TDApiRequest.urlEncode;
import static com.treasuredata.client.TDClientException.ErrorType.CLIENT_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_INPUT;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;
import static com.treasuredata.client.TDClientException.ErrorType.PROXY_AUTHENTICATION_FAILURE;
import static com.treasuredata.client.TDClientException.ErrorType.SERVER_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.UNEXPECTED_RESPONSE_CODE;
import static com.treasuredata.client.TDClientHttpTooManyRequestsException.TOO_MANY_REQUESTS_429;

/**
 * An extension of Jetty HttpClient with request retry handler
 */
public class TDHttpClient
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(TDHttpClient.class);

    @VisibleForTesting
    static final ThreadLocal<SimpleDateFormat> HTTP_DATE_FORMAT = new ThreadLocal<SimpleDateFormat>()
    {
        @Override
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
        }
    };

    // A regex pattern that matches a TD1 apikey without the "TD1 " prefix.
    private static final Pattern NAKED_TD1_KEY_PATTERN = Pattern.compile("^(?:[1-9][0-9]*/)?[a-f0-9]{40}$");

    protected final TDClientConfig config;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    @VisibleForTesting final Multimap<String, String> headers;

    public TDHttpClient(TDClientConfig config)
    {
        this.config = config;
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.connectTimeout(config.connectTimeoutMillis, TimeUnit.MILLISECONDS);
        builder.readTimeout(config.readTimeoutMillis, TimeUnit.MILLISECONDS);
        CookieManager cookieManager = new CookieManager(null, CookiePolicy.ACCEPT_ORIGINAL_SERVER);
        builder.cookieJar(new JavaNetCookieJar(cookieManager));

        // Proxy configuration
        if (config.proxy.isPresent()) {
            final ProxyConfig proxyConfig = config.proxy.get();
            logger.trace("proxy configuration: " + proxyConfig);
            // TODO Support https proxy
            builder.proxy(new Proxy(Proxy.Type.HTTP, InetSocketAddress.createUnresolved(proxyConfig.getHost(), proxyConfig.getPort())));

            if (proxyConfig.requireAuthentication()) {
                builder.proxyAuthenticator(new ProxyAuthenticator(proxyConfig));
            }
        }
        // connection pool
        ConnectionPool connectionPool = new ConnectionPool(config.connectionPoolSize, 5, TimeUnit.MINUTES);
        builder.connectionPool(connectionPool);

        // Build OkHttpClient
        this.httpClient = builder.build();
        this.headers = ImmutableMultimap.copyOf(config.headers);

//        if (config.requestBufferSize.isPresent()) {
//            httpClient.setRequestBufferSize(config.requestBufferSize.get());
//        }
//        if (config.responseBufferSize.isPresent()) {
//            httpClient.setResponseBufferSize(config.responseBufferSize.get());
//        }

        // Prepare jackson json-object mapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JsonOrgModule()) // for mapping query json strings into JSONObject
                .registerModule(new GuavaModule())   // for mapping to Guava Optional class
                .configure(DeserializationFeature.UNWRAP_ROOT_VALUE, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private TDHttpClient(TDClientConfig config, OkHttpClient httpClient, ObjectMapper objectMapper, Multimap<String, String> headers)
    {
        this.config = config;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.headers = headers;
    }

    /**
     * Get a {@link TDHttpClient} that uses the specified headers for each request. Reuses the same
     * underlying http client so closing the returned instance will return this instance as well.
     *
     * @param headers
     * @return
     */
    public TDHttpClient withHeaders(Multimap<String, String> headers)
    {
        Multimap<String, String> mergedHeaders = ImmutableMultimap.<String, String>builder()
                .putAll(this.headers)
                .putAll(headers)
                .build();
        return new TDHttpClient(config, httpClient, objectMapper, mergedHeaders);
    }

    ObjectMapper getObjectMapper()
    {
        return objectMapper;
    }

    public void close()
    {
        // No need to close OkHttp client
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

    protected Request.Builder setTDAuthHeaders(Request.Builder request, String dateHeader)
    {
        // Do nothing
        return request;
    }

    /**
     * Making this protected to allow overiding this value
     *
     * @return
     */
    protected String getClientName()
    {
        return "td-client-java " + TDClient.getVersion();
    }

    private static MediaType mediaTypeJson = MediaType.parse("application/json");
    private static MediaType mediaTypeXwwwFormUrlencoded = MediaType.parse("application/x-www-form-urlencoded");
    private static MediaType mediaTypeOctetStream = MediaType.parse("application/octet-stream");

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
            if (apiRequest.getMethod() == TDHttpMethod.GET ||
                    (apiRequest.getMethod() == TDHttpMethod.POST && apiRequest.getPostJson().isPresent())) {
                requestUri += "?" + queryStr;
            }
        }

        logger.debug("Sending API request to {}", requestUri);
        String dateHeader = RFC2822_FORMAT.get().format(new Date());
        Request.Builder request =
                new Request.Builder()
                        .url(requestUri)
                        .header("User-Agent", getClientName())
                        .header("Date", dateHeader);

        request = setTDAuthHeaders(request, dateHeader);

        // Set API Key
        Optional<String> apiKey = apiKeyCache.or(config.apiKey);
        if (apiKey.isPresent()) {
            String auth;
            if (isNakedTD1Key(apiKey.get())) {
                auth = "TD1 " + apiKey.get();
            }
            else {
                auth = apiKey.get();
            }
            request.header("Authorization", auth);
        }

        // Set other headers
        for (Map.Entry<String, String> entry : headers.entries()) {
            request.header(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : apiRequest.getHeaderParams().entries()) {
            request.header(entry.getKey(), entry.getValue());
        }

        // Submit method specific headers
        switch (apiRequest.getMethod()) {
            case GET:
                request = request.get();
            case DELETE:
                request = request.delete();
            case POST:
                if (apiRequest.getPostJson().isPresent()) {
                    request = request.post(RequestBody.create(mediaTypeJson, apiRequest.getPostJson().get()));
                }
                else if (queryStr.length() > 0) {
                    request = request.post(RequestBody.create(mediaTypeXwwwFormUrlencoded, queryStr));
                }
                else {
                    // We should set content-length explicitly for an empty post
                    request = request.header("Content-Length", "0");
                }
                break;
            case PUT:
                if (apiRequest.getPutFile().isPresent()) {
                    try {
                        request = request.put(RequestBody.create(mediaTypeOctetStream, apiRequest.getPutFile().get()));
                    }
                    catch (NullPointerException e) {
                        throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, "Failed to read input file: " + apiRequest.getPutFile().get());
                    }
                }
                break;
        }

        // OkHttp will follow redirect (302)

        return request.build();
    }

    private static boolean isNakedTD1Key(String s)
    {
        return NAKED_TD1_KEY_PATTERN.matcher(s).matches();
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
                    long waitTimeMillis = calculateWaitTimeMillis(backoff, rootCause);
                    logger.warn(String.format("Retrying request to %s (%d/%d) in %.2f sec.", apiRequest.getPath(), backoff.getExecutionCount(), retryLimit, waitTimeMillis / 1000.0));
                    Thread.sleep(waitTimeMillis);
                }

                ResponseType response = null;
                try {
                    Request request = prepareRequest(apiRequest, apiKeyCache);
                    response = handler.submit(httpClient, request);
                    int code = response.code();
                    if (handler.isSuccess(response)) {
                        // 2xx success
                        logger.debug(String.format("[%d:%s] API request to %s has succeeded", code, HttpStatus.getMessage(code), apiRequest.getPath()));
                        return handler.onSuccess(response);
                    }
                    else {
                        byte[] returnedContent = handler.onError(response);
                        rootCause = Optional.of(handleHttpResponseError(apiRequest.getPath(), code, returnedContent, response));
                    }
                }
                catch (IOException e) {
                    logger.warn("API request failed", e);
                    if (e.getCause() instanceof EOFException) {
                        // Jetty returns EOFException when the connection was interrupted
                        rootCause = Optional.<TDClientException>of(new TDClientInterruptedException("connection failure (EOFException)", (EOFException) e.getCause()));
                    }
                    else if (e.getCause() instanceof TimeoutException) {
                        rootCause = Optional.<TDClientException>of(new TDClientTimeoutException((TimeoutException) e.getCause()));
                    }
                    else if (e.getCause() instanceof SocketException) {
                        final SocketException causeSocket = (SocketException) e.getCause();
                        if (causeSocket instanceof BindException ||
                                causeSocket instanceof ConnectException ||
                                causeSocket instanceof NoRouteToHostException ||
                                causeSocket instanceof PortUnreachableException) {
                            // All known SocketException are retryable.
                            rootCause = Optional.<TDClientException>of(new TDClientSocketException(causeSocket));
                        }
                        else {
                            // Other unknown SocketException are considered non-retryable.
                            throw new TDClientSocketException(causeSocket);
                        }
                    }
                    else if (e.getCause() instanceof SSLException) {
                        SSLException cause = (SSLException) e.getCause();
                        if (cause instanceof SSLHandshakeException || cause instanceof SSLKeyException || cause instanceof SSLPeerUnverifiedException) {
                            // deterministic SSL exceptions
                            throw new TDClientSSLException(cause);
                        }
                        else {
                            // SSLProtocolException and uncategorized SSL exceptions (SSLException) such as unexpected_message may be retryable
                            rootCause = Optional.<TDClientException>of(new TDClientSSLException(cause));
                        }
                    }
                    else if (e.getCause() instanceof TimeoutException) {
                        logger.warn(String.format("API request to %s has timed out", apiRequest.getPath()), e);
                        rootCause = Optional.<TDClientException>of(new TDClientTimeoutException(e));
                    }
                    else {
                        throw new TDClientProcessingException(e);
                    }
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

    private long calculateWaitTimeMillis(ExponentialBackOff backoff, Optional<TDClientException> rootCause)
    {
        long waitTimeMillis = backoff.nextWaitTimeMillis();
        if (rootCause.isPresent() && rootCause.get() instanceof TDClientHttpException) {
            TDClientHttpException httpException = (TDClientHttpException) rootCause.get();
            Optional<Date> retryAfter = httpException.getRetryAfter();
            if (retryAfter.isPresent()) {
                long maxWaitMillis = config.retryLimit * config.retryMaxIntervalMillis;
                long now = System.currentTimeMillis();
                long retryAfterMillis = retryAfter.get().getTime() - now;
                // Bound the wait so we do not end up sleeping forever just because the server told us to.
                if (retryAfterMillis > maxWaitMillis) {
                    throw httpException;
                }
                waitTimeMillis = Math.max(waitTimeMillis, retryAfterMillis);
            }
        }
        return waitTimeMillis;
    }

    private TDClientException handleHttpResponseError(String apiRequestPath, int code, byte[] returnedContent, Response response)
    {
        long now = System.currentTimeMillis();
        Date retryAfter = parseRetryAfter(now, response);
        Optional<TDApiErrorMessage> errorResponse = parseErrorResponse(returnedContent);
        String responseErrorText = errorResponse.isPresent() ? ": " + errorResponse.get().getText() : "";
        String errorMessage = String.format("[%d:%s] API request to %s has failed%s", code, HttpStatus.getMessage(code), apiRequestPath, responseErrorText);
        if (HttpStatus.isClientError(code)) {
            logger.debug(errorMessage);
            switch (code) {
                // Soft 4xx errors. These we retry.
                case TOO_MANY_REQUESTS_429:
                    return new TDClientHttpTooManyRequestsException(errorMessage, retryAfter);

                // Hard 4xx error. We do not retry the execution on this type of error
                case HttpStatus.UNAUTHORIZED_401:
                    throw new TDClientHttpUnauthorizedException(errorMessage);
                case HttpStatus.NOT_FOUND_404:
                    throw new TDClientHttpNotFoundException(errorMessage);
                case HttpStatus.CONFLICT_409:
                    String conflictsWith = errorResponse.isPresent() ? parseConflictsWith(errorResponse.get()) : null;
                    throw new TDClientHttpConflictException(errorMessage, conflictsWith);
                case HttpStatus.PROXY_AUTHENTICATION_REQUIRED_407:
                    throw new TDClientHttpException(PROXY_AUTHENTICATION_FAILURE, errorMessage, code, retryAfter);
                case HttpStatus.UNPROCESSABLE_ENTITY_422:
                    throw new TDClientHttpException(INVALID_INPUT, errorMessage, code, retryAfter);
                default:
                    throw new TDClientHttpException(CLIENT_ERROR, errorMessage, code, retryAfter);
            }
        }
        logger.warn(errorMessage);
        if (HttpStatus.isServerError(code)) {
            // Just returns exception info for 5xx errors
            return new TDClientHttpException(SERVER_ERROR, errorMessage, code, retryAfter);
        }
        else {
            throw new TDClientHttpException(UNEXPECTED_RESPONSE_CODE, errorMessage, code, retryAfter);
        }
    }

    /**
     * https://tools.ietf.org/html/rfc7231#section-7.1.3
     */
    @VisibleForTesting
    static Date parseRetryAfter(long now, Response response)
    {
        String retryAfter = response.header("Retry-After");
        if (retryAfter == null) {
            return null;
        }
        // Try parsing as a number of seconds first
        try {
            long retryAfterSeconds = Long.parseLong(retryAfter);
            return new Date(now + TimeUnit.SECONDS.toMillis(retryAfterSeconds));
        }
        catch (NumberFormatException e) {
            // Then try parsing as a HTTP-date
            try {
                return HTTP_DATE_FORMAT.get().parse(retryAfter);
            }
            catch (ParseException ignore) {
                logger.warn("Failed to parse Retry-After header: '" + retryAfter + "'");
                return null;
            }
        }
    }

    private String parseConflictsWith(TDApiErrorMessage errorResponse)
    {
        Map<String, Object> details = errorResponse.getDetails();
        if (details == null) {
            return null;
        }
        Object conflictsWith = details.get("conflicts_with");
        if (conflictsWith == null) {
            return null;
        }
        return String.valueOf(conflictsWith);
    }

    public String call(TDApiRequest apiRequest, Optional<String> apiKeyCache)
    {
        Response response = submitRequest(apiRequest, apiKeyCache, new DefaultContentHandler());
        try {
            String content = response.body().string();
            if (logger.isTraceEnabled()) {
                logger.trace("response:\n{}", content);
            }
            return content;
        }
        catch (IOException e) {
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
    }

    public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final Function<InputStream, Result> contentStreamHandler)
    {
        Response response = submitRequest(apiRequest, apiKeyCache, new DefaultContentHandler());
        return contentStreamHandler.apply(response.body().byteStream());
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
        return call(apiRequest, apiKeyCache, objectMapper.getTypeFactory().constructType(resultType));
    }

    /**
     * Submit an API request, and bind the returned JSON data into an object of the given result type reference.
     * For mapping it uses Jackson object mapper.
     *
     * @param apiRequest
     * @param resultType
     * @param <Result>
     * @return
     * @throws TDClientException
     */
    public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final TypeReference<Result> resultType)
            throws TDClientException
    {
        return call(apiRequest, apiKeyCache, objectMapper.getTypeFactory().constructType(resultType));
    }

    /**
     * Submit an API request, and bind the returned JSON data into an object of the given result jackson JavaType.
     * For mapping it uses Jackson object mapper.
     *
     * @param apiRequest
     * @param resultType
     * @param <Result>
     * @return
     * @throws TDClientException
     */
    @SuppressWarnings(value = "unchecked cast")
    public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final JavaType resultType)
            throws TDClientException
    {
        try {
            Response response = submitRequest(apiRequest, apiKeyCache, new DefaultContentHandler());
            byte[] content = response.body().bytes();
            if (logger.isTraceEnabled()) {
                logger.trace("response:\n{}", new String(content, StandardCharsets.UTF_8));
            }
            if (resultType.getRawClass() == String.class) {
                return (Result) new String(content, StandardCharsets.UTF_8);
            }
            else {
                return getJsonReader(resultType).readValue(content);
            }
        }
        catch (JsonMappingException e) {
            logger.error("Jackson mapping error", e);
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
        catch (IOException e) {
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
    }

    private ObjectReader getJsonReader(final JavaType type)
    {
        ObjectReader reader = objectMapper.readerFor(type);
        if (type.getContentType() != null) {
            JsonCollectionRootName rootName = type.getContentType().getRawClass().getAnnotation(JsonCollectionRootName.class);
            if (rootName != null) {
                reader = reader.withRootName(rootName.value());
            }
        }
        else {
            JsonRootName rootName = type.getRawClass().getAnnotation(JsonRootName.class);
            if (rootName != null) {
                reader = reader.withRootName(rootName.value());
            }
        }
        return reader;
    }

    public static interface Handler<ResponseType extends Response, Result>
    {
        ResponseType submit(OkHttpClient client, Request request)
                throws IOException;

        Result onSuccess(ResponseType response);

        /**
         * @param response
         * @return returned content
         */
        byte[] onError(ResponseType response);

        boolean isSuccess(ResponseType response);
    }

    public static class DefaultContentHandler
            implements Handler<Response, Response>
    {
        public DefaultContentHandler()
        {
        }

        @Override
        public Response submit(OkHttpClient client, Request request)
                throws IOException
        {
            return client.newCall(request).execute();
        }

        @Override
        public Response onSuccess(Response response)
        {
            return response;
        }

        @Override
        public byte[] onError(Response response)
        {
            try {
                return response.body().bytes();
            }
            catch (IOException e) {
                throw new TDClientException(INVALID_JSON_RESPONSE, e);
            }
        }

        @Override
        public boolean isSuccess(Response response)
        {
            return HttpStatus.isSuccess(response.code());
        }
    }
}
