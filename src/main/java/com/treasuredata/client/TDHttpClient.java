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
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.treasuredata.client.model.TDApiError;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.B64Code;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jetty.connector.JettyClientProperties;
import org.glassfish.jersey.jetty.connector.JettyConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.treasuredata.client.TDClientException.ErrorType.CLIENT_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;
import static com.treasuredata.client.TDClientException.ErrorType.PROXY_AUTHENTICATION_REQUIRED;
import static com.treasuredata.client.TDClientException.ErrorType.RESPONSE_READ_FAILURE;
import static com.treasuredata.client.TDClientException.ErrorType.SERVER_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.UNEXPECTED_RESPONSE_CODE;

/**
 * An extension of Jetty HttpClient with request retry handler
 */
public class TDHttpClient
{
    private static final Logger logger = LoggerFactory.getLogger(TDHttpClient.class);
    private final TDClientConfig config;
    private final Client httpClient;
    private final ObjectMapper objectMapper;
    private Optional<String> credentialCache = Optional.absent();

    public TDHttpClient(TDClientConfig config)
    {
        this.config = config;

        ClientConfig httpConfig = new ClientConfig();

        // We need to use Jetty connector to support proxy requests
        httpConfig.connectorProvider(new JettyConnectorProvider());

        // Basic http client configurations
        httpConfig
                .property(ClientProperties.CONNECT_TIMEOUT, config.getConnectTimeoutMillis())
                .property(ClientProperties.READ_TIMEOUT, config.getConnectTimeoutMillis());

        // Jetty specific configuration. Disable cookie
        httpConfig.property(JettyClientProperties.DISABLE_COOKIES, true);

        // Configure proxy server
        if (config.getProxy().isPresent()) {
            ProxyConfig proxyConfig = config.getProxy().get();
            logger.info("proxy configuration: " + proxyConfig);
            httpConfig.property(ClientProperties.PROXY_URI, proxyConfig.getUri());
        }

        // Prepare jackson json-object mapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JsonOrgModule()) // for mapping query json strings into JSONObject
                .registerModule(new GuavaModule())   // for mapping to Guava Optional class
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JacksonJsonProvider jacksonJsonProvider = new JacksonJsonProvider(objectMapper);
        httpConfig.register(jacksonJsonProvider);

        this.httpClient = ClientBuilder.newClient(httpConfig);
    }

    public void close()
    {
        try {
            httpClient.close();
        }
        catch (Exception e) {
            logger.error("Failed to terminate Jetty client", e);
            throw Throwables.propagate(e);
        }
    }

    protected Optional<TDApiError> parseErrorResponse(byte[] content)
    {
        try {
            if (content.length > 0 && content[0] == '{') {
                // Error message from TD API
                return Optional.of(objectMapper.readValue(content, TDApiError.class));
            }
            else {
                // Error message from Proxy server etc.
                String contentStr = new String(content);
                return Optional.of(new TDApiError("error", contentStr, "error"));
            }
        }
        catch (IOException e) {
            logger.warn(String.format("Failed to parse error response: %s", new String(content)), e);
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
    private static final ThreadLocal<MessageDigest> SHA1 =
            new ThreadLocal<MessageDigest>()
            {
                @Override
                protected MessageDigest initialValue()
                {
                    try {
                        return MessageDigest.getInstance("SHA-1");
                    }
                    catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException("SHA-1 digest algorithm must be available but not found", e);
                    }
                }
            };
    private static final char[] hexChars = new char[16];

    static {
        for (int i = 0; i < 16; i++) {
            hexChars[i] = Integer.toHexString(i).charAt(0);
        }
    }

    @VisibleForTesting
    static String sha1HexFromString(String string)
    {
        MessageDigest sha1 = SHA1.get();
        sha1.reset();
        sha1.update(string.getBytes());
        byte[] bytes = sha1.digest();

        // convert binary to hex string
        char[] array = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = (int) bytes[i];
            array[i * 2] = hexChars[(b & 0xf0) >> 4];
            array[i * 2 + 1] = hexChars[b & 0x0f];
        }
        return new String(array);
    }

    public Response submitRequest(TDApiRequest apiRequest, Optional<String> apiKeyOverwrite)
    {
        String queryStr = "";
        String requestUri = String.format("%s%s%s", config.getHttpScheme(), config.getEndpoint(), apiRequest.getPath());
        WebTarget target = httpClient.target(requestUri);
        if (!apiRequest.getQueryParams().isEmpty()) {
            List<String> queryParamList = new ArrayList<String>(apiRequest.getQueryParams().size());
            for (Map.Entry<String, String> queryParam : apiRequest.getQueryParams().entrySet()) {
                target = target.queryParam(queryParam.getKey(), queryParam.getValue());
                queryParamList.add(String.format("%s=%s", queryParam.getKey(), queryParam.getValue()));
            }
            queryStr = Joiner.on("&").join(queryParamList);
        }
        Invocation.Builder request = target.request()
                .header(HttpHeaders.USER_AGENT, "TDClient " + TDClient.getVersion())
                .header(HttpHeaders.DATE, RFC2822_FORMAT.get().format(new Date()));

        // Set API Key
        Optional<String> apiKey = apiKeyOverwrite.or(config.getApiKey());
        if (apiKey.isPresent()) {
            request.header(HttpHeaders.AUTHORIZATION, "TD1 " + apiKey.get());
        }
        else {
            logger.warn("no API key is found");
        }

        // Set proxy
        if(config.getProxy().isPresent()) {
            ProxyConfig proxy = config.getProxy().get();
            request.header("Proxy-Authorization", "Basic " + B64Code.encode(proxy.getUser() + ":" + proxy.getPassword(), StandardCharsets.ISO_8859_1));
        }

        // Set other headers
        for (Map.Entry<String, String> entry : apiRequest.getHeaderParams().entrySet()) {
            request.header(entry.getKey(), entry.getValue());
        }

        // Submit the request
        switch (apiRequest.getMethod()) {
            case GET:
                return request.get();
            case POST:
                if (queryStr != null && queryStr.length() > 0) {
                    return request.post(Entity.entity(queryStr, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
                }
                else {
                    // We should set content-length explicitely for an empty post
                    request.header("Content-Length", "0");
                    return request.post(Entity.entity("", MediaType.APPLICATION_FORM_URLENCODED_TYPE));
                }
            case PUT:
                if (apiRequest.getPutFile().isPresent()) {
                    return request.put(Entity.entity(apiRequest.getPutFile().get(), MediaType.APPLICATION_OCTET_STREAM_TYPE));
                }
                break;
        }
        return request.build(apiRequest.getMethod().asString()).invoke();
    }

    public <Result> Result submitRequest(TDApiRequest apiRequest, Function<Response, Result> handler)
            throws TDClientException
    {
        ExponentialBackOffRetry retry = new ExponentialBackOffRetry(config.getRetryLimit(), config.getRetryInitialWaitMillis(), config.getRetryIntervalMillis());
        Optional<TDClientException> rootCause = Optional.absent();
        try {
            Optional<Integer> nextInterval = Optional.absent();
            do {
                if (retry.getExecutionCount() > 0) {
                    int waitTimeMillis = nextInterval.get();
                    logger.warn(String.format("Retrying request to %s (%d/%d) in %.2f sec.", apiRequest.getPath(), retry.getExecutionCount(), retry.getMaxRetryCount(), waitTimeMillis / 1000.0));
                    Thread.sleep(waitTimeMillis);
                }

                Response response = null;
                try {
                    logger.debug("Sending API request to {}", apiRequest.getPath());
                    response = submitRequest(apiRequest, credentialCache);
                    int code = response.getStatus();
                    if (HttpStatus.isSuccess(code)) {
                        // 2xx success
                        logger.info(String.format("[%d:%s] API request to %s has succeeded", code, HttpStatus.getMessage(code), apiRequest.getPath()));
                        return handler.apply(response);
                    }
                    else {
                        byte[] returnedContent = response.readEntity(byte[].class);
                        Optional<TDApiError> errorResponse = parseErrorResponse(returnedContent);
                        String responseErrorText = errorResponse.isPresent() ? ": " + errorResponse.get().getText() : "";
                        String errorMessage = String.format("[%d:%s] API request to %s has failed%s", code, HttpStatus.getMessage(code), apiRequest.getPath(), responseErrorText);
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
                                    throw new TDClientHttpException(PROXY_AUTHENTICATION_REQUIRED, errorMessage, code);
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
                catch (ProcessingException e) {
                    logger.warn("API request failed", e);
                    rootCause = Optional.<TDClientException>of(new TDClientProcessingException(e));
                }
                finally {
                    if (response != null) {
                        response.close();
                    }
                }
            }
            while ((nextInterval = retry.nextWaitTimeMillis()).isPresent());
        }
        catch (InterruptedException e) {
            logger.warn("API request interrupted", e);
            throw new TDClientInterruptedException(e);
        }
        logger.warn("API request retry limit exceeded: ({}/{})", config.getRetryLimit(), config.getRetryLimit());

        checkState(rootCause.isPresent(), "rootCause must be present here");
        // Throw the last seen error
        throw rootCause.get();
    }

    public String call(TDApiRequest apiRequest)
    {
        return submitRequest(apiRequest, new Function<Response, String>()
        {
            @Override
            public String apply(Response input)
            {
                String response = input.readEntity(String.class);
                if (logger.isTraceEnabled()) {
                    logger.trace("response:\n{}", response);
                }
                return response;
            }
        });
    }

    public <Result> Result call(TDApiRequest apiRequest, final Function<InputStream, Result> contentStreamHandler)
    {
        return submitRequest(apiRequest, new Function<Response, Result>()
        {
            @Override
            public Result apply(Response input)
            {
                return contentStreamHandler.apply(input.readEntity(InputStream.class));
            }
        });
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
    public <Result> Result call(TDApiRequest apiRequest, final Class<Result> resultType)
            throws TDClientException
    {
        return submitRequest(apiRequest, new Function<Response, Result>()
        {
            @Override
            public Result apply(Response input)
            {
                try {
                    byte[] response = input.readEntity(byte[].class);
                    if (logger.isTraceEnabled()) {
                        logger.trace("response:\n{}", new String(response));
                    }
                    return objectMapper.readValue(response, resultType);
                }
                catch (JsonMappingException e) {
                    logger.error("Jackson mapping error", e);
                    throw new TDClientException(INVALID_JSON_RESPONSE, e);
                }
                catch (IOException e) {
                    throw new TDClientException(RESPONSE_READ_FAILURE, e);
                }
            }
        });
    }

    public void setCredentialCache(String apikey)
    {
        this.credentialCache = Optional.of(apikey);
    }
}
