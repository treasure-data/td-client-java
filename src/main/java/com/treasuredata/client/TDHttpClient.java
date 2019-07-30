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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.treasuredata.client.impl.ProxyAuthenticator;
import com.treasuredata.client.model.JsonCollectionRootName;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.internal.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.DATE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.treasuredata.client.TDApiRequest.urlEncode;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;
import static com.treasuredata.client.TDHttpRequestHandler.ResponseContext;
import static com.treasuredata.client.TDHttpRequestHandlers.byteArrayContentHandler;
import static com.treasuredata.client.TDHttpRequestHandlers.newByteStreamHandler;
import static com.treasuredata.client.TDHttpRequestHandlers.stringContentHandler;

/**
 * An extension of Jetty HttpClient with request retry handler
 */
public class TDHttpClient
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(TDHttpClient.class);

    // Used for reading JSON response
    static ObjectMapper defaultObjectMapper = new ObjectMapper()
            .registerModule(new JsonOrgModule()) // for mapping query json strings into JSONObject
            .registerModule(new GuavaModule())   // for mapping to Guava Optional class
            .registerModule(new Jdk8Module())    // for handling java.util.Optional correctly
            .configure(DeserializationFeature.UNWRAP_ROOT_VALUE, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // A regex pattern that matches a TD1 apikey without the "TD1 " prefix.
    private static final Pattern NAKED_TD1_KEY_PATTERN = Pattern.compile("^(?:[1-9][0-9]*/)?[a-f0-9]{40}$");

    protected final TDClientConfig config;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    @VisibleForTesting final Multimap<String, String> headers;

    public TDHttpClient(TDClientConfig config)
    {
        this.config = config;

        // Prepare OkHttpClient
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.connectTimeout(config.connectTimeoutMillis, TimeUnit.MILLISECONDS);
        builder.readTimeout(config.readTimeoutMillis, TimeUnit.MILLISECONDS);

        // Proxy configuration
        if (config.proxy.isPresent()) {
            final ProxyConfig proxyConfig = config.proxy.get();
            logger.trace("proxy configuration: " + proxyConfig);
            // TODO Support https proxy
            builder.proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyConfig.getHost(), proxyConfig.getPort())));

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

        // Prepare jackson json-object mapper
        this.objectMapper = defaultObjectMapper;
    }

    protected TDHttpClient(TDHttpClient reference)
    {
        this(reference.config, reference.httpClient, reference.objectMapper, reference.headers);
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
        // Cleanup the internal thread manager and connections
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
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
        String portStr = config.port.map(new Function<Integer, String>()
        {
            @Override
            public String apply(Integer input)
            {
                return ":" + input.toString();
            }
        }).orElse("");
        String requestUri = apiRequest.getPath().startsWith("http")
                ? apiRequest.getPath()
                : String.format("%s://%s%s%s", config.useSSL ? "https" : "http", config.endpoint, portStr, apiRequest.getPath());

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

        if (logger.isDebugEnabled()) {
            logger.debug("Sending API request to {}", requestUri);
        }
        String dateHeader = RFC2822_FORMAT.get().format(new Date());
        Request.Builder request =
                new Request.Builder()
                        .url(requestUri)
                        .header(USER_AGENT, getClientName())
                        .header(DATE, dateHeader);

        request = setTDAuthHeaders(request, dateHeader);

        // Set other headers
        for (Map.Entry<String, String> entry : headers.entries()) {
            request = request.addHeader(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : apiRequest.getHeaderParams().entries()) {
            request = request.addHeader(entry.getKey(), entry.getValue());
        }

        // Set API Key after setting the other headers
        Optional<String> apiKey = apiKeyCache.isPresent() ? apiKeyCache : config.apiKey;
        if (apiKey.isPresent()) {
            String auth;
            if (isNakedTD1Key(apiKey.get())) {
                auth = "TD1 " + apiKey.get();
            }
            else {
                auth = apiKey.get();
            }
            request = request.header(AUTHORIZATION, auth);
        }

        // Submit method specific headers
        switch (apiRequest.getMethod()) {
            case GET:
                request = request.get();
                break;
            case DELETE:
                request = request.delete();
                break;
            case POST:
                if (apiRequest.getPostJson().isPresent()) {
                    request = request.post(createRequestBodyWithoutCharset(mediaTypeJson, apiRequest.getPostJson().get()));
                }
                else if (queryStr.length() > 0) {
                    request = request.post(createRequestBodyWithoutCharset(mediaTypeXwwwFormUrlencoded, queryStr));
                }
                else {
                    // We should set content-length explicitly for an empty post
                    request = request
                            .header(CONTENT_LENGTH, "0")
                            .post(RequestBody.create(null, ""));
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
                else if (apiRequest.getContent().isPresent()) {
                    try {
                        request = request.put(RequestBody.create(mediaTypeOctetStream, apiRequest.getContent().get(), apiRequest.getContentOffset(), apiRequest.getContentLength()));
                    }
                    catch (Throwable e) {
                        throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, "Failed to get Content");
                    }
                }
                break;
        }

        // OkHttp will follow redirect (302)

        return request.build();
    }

    private static RequestBody createRequestBodyWithoutCharset(MediaType contentType, String content)
    {
        Charset charset = Util.UTF_8;
        if (contentType != null) {
            charset = contentType.charset();
            if (charset == null) {
                charset = Util.UTF_8;
            }
        }
        byte[] bytes = content.getBytes(charset);
        return RequestBody.create(contentType, bytes);
    }

    private static boolean isNakedTD1Key(String s)
    {
        return NAKED_TD1_KEY_PATTERN.matcher(s).matches();
    }

    protected static class RequestContext
    {
        private final ExponentialBackOff backoff;
        public final TDApiRequest apiRequest;
        public final Optional<String> apiKeyCache;
        public final Optional<TDClientException> rootCause;

        public RequestContext(TDClientConfig config, TDApiRequest apiRequest, Optional<String> apiKeyCache)
        {
            this(new ExponentialBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier), apiRequest, apiKeyCache, Optional.empty());
        }

        public RequestContext(ExponentialBackOff backoff, TDApiRequest apiRequest, Optional<String> apiKeyCache, Optional<TDClientException> rootCause)
        {
            this.backoff = backoff;
            this.apiRequest = apiRequest;
            this.apiKeyCache = apiKeyCache;
            this.rootCause = rootCause;
        }

        public RequestContext withTDApiRequest(TDApiRequest newApiRequest)
        {
            return new RequestContext(backoff, newApiRequest, apiKeyCache, rootCause);
        }

        public RequestContext withRootCause(TDClientException e)
        {
            return new RequestContext(backoff, apiRequest, apiKeyCache, Optional.of(e));
        }
    }

    protected <Result> Result submitRequest(RequestContext context, TDHttpRequestHandler<Result> handler)
            throws TDClientException, InterruptedException
    {
        int executionCount = context.backoff.getExecutionCount();
        if (executionCount > config.retryLimit) {
            logger.warn("API request retry limit exceeded: ({}/{})", config.retryLimit, config.retryLimit);

            checkState(context.rootCause.isPresent(), "rootCause must be present here");
            // Throw the last seen error
            throw context.rootCause.get();
        }
        else {
            if (executionCount == 0) {
                // First attempt
                context.backoff.incrementExecutionCount();
            }
            else {
                // Requst retry
                long waitTimeMillis = calculateWaitTimeMillis(context.backoff.nextWaitTimeMillis(), context.rootCause);
                logger.warn(String.format("Retrying request to %s (%d/%d) in %.2f sec.", context.apiRequest.getPath(), executionCount, config.retryLimit, waitTimeMillis / 1000.0));
                // Sleeping for a while. This may throw InterruptedException
                Thread.sleep(waitTimeMillis);
            }

            try {
                // Prepare http request
                Request request = prepareRequest(context.apiRequest, context.apiKeyCache);
                // Apply request customization
                request = handler.prepareRequest(request);

                // Get response
                try (Response response = handler.send(httpClient, request)) {
                    int code = response.code();
                    // Retry upon proxy authentication request
                    // This is a workaround for this issue: https://github.com/square/okhttp/issues/3111
                    if (code == HttpStatus.TEMPORARY_REDIRECT_307 || code == 308) {
                        String location = response.header(LOCATION);
                        if (location != null) {
                            context = context.withTDApiRequest(context.apiRequest.withUri(location));
                            return submitRequest(context, handler);
                        }
                    }

                    ResponseContext responseContext = new ResponseContext(context.apiRequest, response);
                    if (handler.isSuccess(responseContext)) {
                        // 2xx success
                        logger.debug(String.format("[%d:%s] API request to %s has succeeded", code, HttpStatus.getMessage(code), context.apiRequest.getPath()));
                        return handler.onSuccess(response);
                    }
                    else {
                        // This may directly throw an TDClientException if we know this is unrecoverable error.
                        context = context.withRootCause(handler.resolveHttpResponseError(responseContext));
                    }
                }
            }
            catch (Exception e) {
                // TDClientHttpException is already handled in TDRequestErrorHandler, so we need to show warning for the other types of error messages
                if (!TDClientHttpException.class.isAssignableFrom(e.getClass())) {
                    logger.warn(String.format("API request to %s failed: %s, cause: %s", context.apiRequest.getPath(), e.getClass(), e.getCause() == null ? e.getMessage() : e.getCause().getClass()), e);
                }
                // This may throw TDClientException if the error is not recoverable
                context = context.withRootCause(handler.resolveError(e));
            }
            return submitRequest(context, handler);
        }
    }

    private long calculateWaitTimeMillis(long nextWaitTimeMillis, Optional<TDClientException> rootCause)
    {
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
                nextWaitTimeMillis = Math.max(nextWaitTimeMillis, retryAfterMillis);
            }
        }
        return nextWaitTimeMillis;
    }

    /**
     * A low-level method to submit a TD API request.
     *
     * @param apiRequest
     * @param apiKeyCache
     * @param handler
     * @param <Result>
     * @return
     * @throws TDClientException
     */
    public <Result> Result submitRequest(TDApiRequest apiRequest, Optional<String> apiKeyCache, TDHttpRequestHandler<Result> handler)
            throws TDClientException
    {
        RequestContext requestContext = new RequestContext(config, apiRequest, apiKeyCache);
        try {
            return submitRequest(requestContext, handler);
        }
        catch (InterruptedException e) {
            logger.warn("API request interrupted", e);
            throw new TDClientInterruptedException(e);
        }
        catch (TDClientException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
    }

    /**
     * Submit an API request and get the result as String value (e.g. json)
     *
     * @param apiRequest
     * @param apiKeyCache
     * @return
     */
    public String call(TDApiRequest apiRequest, Optional<String> apiKeyCache)
    {
        String content = submitRequest(apiRequest, apiKeyCache, stringContentHandler);
        if (logger.isTraceEnabled()) {
            logger.trace("response:\n{}", content);
        }
        return content;
    }

    /**
     * Submit an API request, and returns the byte InputStream. This stream is valid until exiting this function.
     *
     * @param apiRequest
     * @param apiKeyCache
     * @param contentStreamHandler
     * @param <Result>
     * @return
     */
    public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final Function<InputStream, Result> contentStreamHandler)
    {
        return submitRequest(apiRequest, apiKeyCache, newByteStreamHandler(contentStreamHandler));
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
            byte[] content = submitRequest(apiRequest, apiKeyCache, byteArrayContentHandler);
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
}
