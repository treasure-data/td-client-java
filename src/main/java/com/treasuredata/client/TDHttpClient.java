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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.collect.Multimap;
import com.treasuredata.client.model.JsonCollectionRootName;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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

    private static final String AUTHORIZATION = "Authorization";
    private static final String CONTENT_LENGTH = "Content-Length";
    private static final String DATE = "Date";
    private static final String LOCATION = "Location";
    private static final String USER_AGENT = "User-Agent";

    // Used for reading JSON response
    static ObjectMapper defaultObjectMapper = new ObjectMapper()
            .registerModule(new JsonOrgModule()) // for mapping query json strings into JSONObject
            .registerModule(new Jdk8Module())
            .configure(DeserializationFeature.UNWRAP_ROOT_VALUE, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // A regex pattern that matches a TD1 apikey without the "TD1 " prefix.
    private static final Pattern NAKED_TD1_KEY_PATTERN = Pattern.compile("^(?:[1-9][0-9]*/)?[a-f0-9]{40}$");

    protected final TDClientConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Visible for testing.
     */
    final Map<String, Collection<String>> headers;

    public TDHttpClient(TDClientConfig config)
    {
        this.config = config;

        // Prepare JDK HttpClient
        HttpClient.Builder builder = HttpClient.newBuilder();
        builder.connectTimeout(Duration.ofMillis(config.connectTimeoutMillis));

        // Proxy configuration
        if (config.proxy.isPresent()) {
            final ProxyConfig proxyConfig = config.proxy.get();
            logger.trace("proxy configuration: " + proxyConfig);

            ProxySelector proxySelector = ProxySelector.of(new InetSocketAddress(proxyConfig.getHost(), proxyConfig.getPort()));
            builder.proxy(proxySelector);

            if (proxyConfig.requireAuthentication()) {
                // Set up proxy authenticator using system properties for JDK HTTP client
                System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
                System.setProperty("jdk.http.auth.proxying.disabledSchemes", "");
                java.net.Authenticator authenticator = new java.net.Authenticator() {
                    @Override
                    protected java.net.PasswordAuthentication getPasswordAuthentication() {
                        if (getRequestorType() == RequestorType.PROXY) {
                            return new java.net.PasswordAuthentication(
                                    proxyConfig.getUser().orElse(""),
                                    proxyConfig.getPassword().orElse("").toCharArray());
                        }
                        return null;
                    }
                };
                builder.authenticator(authenticator);
            }
        }

        // Build HttpClient
        this.httpClient = builder.build();
        this.headers = config.headersV2;

        // Prepare jackson json-object mapper
        this.objectMapper = defaultObjectMapper;
    }

    protected TDHttpClient(TDHttpClient reference)
    {
        this(reference.config, reference.httpClient, reference.objectMapper, reference.headers);
    }

    private TDHttpClient(TDClientConfig config, HttpClient httpClient, ObjectMapper objectMapper, Map<String, Collection<String>> headers)
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
     * @deprecated Use {@link #withHeaders(Map)} instead.
     */
    @Deprecated
    public TDHttpClient withHeaders(Multimap<String, String> headers)
    {
        return withHeaders(headers.asMap());
    }

    /**
     * Get a {@link TDHttpClient} that uses the specified headers for each request. Reuses the same
     * underlying http client so closing the returned instance will return this instance as well.
     *
     * @param headers
     * @return
     */
    public TDHttpClient withHeaders(Map<String, ? extends Collection<String>> headers)
    {
        Map<String, Collection<String>> mergedHeaders = new HashMap<>(this.headers);
        mergedHeaders.putAll(headers);
        return new TDHttpClient(config, httpClient, objectMapper, Collections.unmodifiableMap(mergedHeaders));
    }

    ObjectMapper getObjectMapper()
    {
        return objectMapper;
    }

    public void close()
    {
        // JDK HTTP client handles cleanup automatically
        // No explicit cleanup needed
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

    protected HttpRequest.Builder setTDAuthHeaders(HttpRequest.Builder requestBuilder, String dateHeader)
    {
        // Do nothing
        return requestBuilder;
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

    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded";
    private static final String CONTENT_TYPE_OCTET_STREAM = "application/octet-stream";

    public HttpRequest prepareRequest(TDApiRequest apiRequest, Optional<String> apiKeyCache)
            throws URISyntaxException
    {
        String queryStr = "";
        String portStr = config.port.map((input) -> ":" + input).orElse("");
        String requestUri = apiRequest.getPath().startsWith("http")
                ? apiRequest.getPath()
                : String.format("%s://%s%s%s", config.useSSL ? "https" : "http", config.endpoint, portStr, apiRequest.getPath());

        if (!apiRequest.getQueryParams().isEmpty()) {
            List<String> queryParamList = new ArrayList<>(apiRequest.getQueryParams().size());
            for (Map.Entry<String, String> queryParam : apiRequest.getQueryParams().entrySet()) {
                queryParamList.add(String.format("%s=%s", urlEncode(queryParam.getKey()), urlEncode(queryParam.getValue())));
            }
            queryStr = String.join("&", queryParamList);
            if (apiRequest.getMethod() == TDHttpMethod.GET ||
                    (apiRequest.getMethod() == TDHttpMethod.POST && apiRequest.getPostJson().isPresent())) {
                requestUri += "?" + queryStr;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Sending API request to {}", requestUri);
        }
        String dateHeader = RFC2822_FORMAT.get().format(new Date());
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(getClientName());
        for (String s : headers.getOrDefault(USER_AGENT, Collections.emptyList())) {
            joiner.add(s);
        }
        String userAgent = joiner.toString();
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(requestUri))
                .header(USER_AGENT, userAgent)
                .header(DATE, dateHeader)
                .timeout(Duration.ofMillis(config.readTimeoutMillis));

        requestBuilder = setTDAuthHeaders(requestBuilder, dateHeader);

        // Set other headers
        for (Map.Entry<String, Collection<String>> e : headers.entrySet()) {
            if (!e.getKey().equals(USER_AGENT)) {
                for (String v : e.getValue()) {
                    requestBuilder = requestBuilder.header(e.getKey(), v);
                }
            }
        }
        for (Map.Entry<String, Collection<String>> entry : apiRequest.getAllHeaders().entrySet()) {
            String k = entry.getKey();
            for (String v : entry.getValue()) {
                requestBuilder = requestBuilder.header(k, v);
            }
        }

        // Set API Key after setting the other headers
        Optional<String> apiKey = Stream.of(apiKeyCache, config.apiKey)
                .flatMap((opt) -> opt.map(Stream::of).orElseGet(Stream::empty))
                .findFirst();
        if (apiKey.isPresent()) {
            String auth;
            if (isNakedTD1Key(apiKey.get())) {
                auth = "TD1 " + apiKey.get();
            }
            else {
                auth = apiKey.get();
            }
            requestBuilder = requestBuilder.header(AUTHORIZATION, auth);
        }

        // Submit method specific headers and build request
        switch (apiRequest.getMethod()) {
            case GET:
                return requestBuilder.GET().build();
            case DELETE:
                return requestBuilder.DELETE().build();
            case POST:
                if (apiRequest.getPostJson().isPresent()) {
                    requestBuilder = requestBuilder.header("Content-Type", CONTENT_TYPE_JSON);
                    return requestBuilder.POST(HttpRequest.BodyPublishers.ofString(apiRequest.getPostJson().get())).build();
                }
                else if (queryStr.length() > 0) {
                    requestBuilder = requestBuilder.header("Content-Type", CONTENT_TYPE_FORM_URLENCODED);
                    return requestBuilder.POST(HttpRequest.BodyPublishers.ofString(queryStr)).build();
                }
                else {
                    // Empty post
                    requestBuilder = requestBuilder.header(CONTENT_LENGTH, "0");
                    return requestBuilder.POST(HttpRequest.BodyPublishers.noBody()).build();
                }
            case PUT:
                if (apiRequest.getPutFile().isPresent()) {
                    try {
                        requestBuilder = requestBuilder.header("Content-Type", CONTENT_TYPE_OCTET_STREAM);
                        return requestBuilder.PUT(HttpRequest.BodyPublishers.ofFile(apiRequest.getPutFile().get().toPath())).build();
                    }
                    catch (Exception e) {
                        throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, "Failed to read input file: " + apiRequest.getPutFile().get());
                    }
                }
                else if (apiRequest.getContent().isPresent()) {
                    try {
                        requestBuilder = requestBuilder.header("Content-Type", CONTENT_TYPE_OCTET_STREAM);
                        byte[] content = apiRequest.getContent().get();
                        int offset = apiRequest.getContentOffset();
                        int length = apiRequest.getContentLength();
                        byte[] slicedContent = new byte[length];
                        System.arraycopy(content, offset, slicedContent, 0, length);
                        return requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(slicedContent)).build();
                    }
                    catch (Throwable e) {
                        throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, "Failed to get Content");
                    }
                }
                else if (queryStr.length() > 0) {
                    requestBuilder = requestBuilder.header("Content-Type", CONTENT_TYPE_FORM_URLENCODED);
                    return requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(queryStr)).build();
                }
                else {
                    // Empty put
                    requestBuilder = requestBuilder.header(CONTENT_LENGTH, "0");
                    return requestBuilder.PUT(HttpRequest.BodyPublishers.noBody()).build();
                }
            default:
                throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, "Unsupported HTTP method: " + apiRequest.getMethod());
        }
    }


    private static boolean isNakedTD1Key(String s)
    {
        return NAKED_TD1_KEY_PATTERN.matcher(s).matches();
    }

    protected static class RequestContext
    {
        private final BackOff backoff;
        public final TDApiRequest apiRequest;
        public final Optional<String> apiKeyCache;
        public final Optional<TDClientException> rootCause;

        public RequestContext(TDClientConfig config, TDApiRequest apiRequest, Optional<String> apiKeyCache)
        {
            this(BackOffStrategy.newBackOff(config), apiRequest, apiKeyCache, Optional.empty());
        }

        public RequestContext(BackOff backoff, TDApiRequest apiRequest, Optional<String> apiKeyCache, Optional<TDClientException> rootCause)
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

            if (context.rootCause.isPresent()) {
                // Throw the last seen error
                throw context.rootCause.get();
            }
            else {
                throw new IllegalStateException("rootCause must be present here");
            }
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
     * @param apiRequest
     * @param apiKeyCache
     * @param contentStreamHandler
     * @param <Result>
     * @return
     * @deprecated Use {@link #call(TDApiRequest, Optional, Function)} instead.
     */
    @Deprecated
    public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final com.google.common.base.Function<InputStream, Result> contentStreamHandler)
    {
        return submitRequest(apiRequest, apiKeyCache, newByteStreamHandler(contentStreamHandler));
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
    @SuppressWarnings(value = "unchecked")
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
