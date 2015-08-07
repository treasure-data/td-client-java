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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.treasuredata.client.api.JettyHttpClient;
import com.treasuredata.client.api.model.TDDatabase;
import com.treasuredata.client.api.model.TDDatabaseList;
import com.treasuredata.client.api.model.TDJob;
import com.treasuredata.client.api.model.TDJobRequest;
import com.treasuredata.client.api.model.TDJobResult;
import com.treasuredata.client.api.model.TDTable;
import com.treasuredata.client.api.model.TDTableList;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class TDClient
        implements TDClientSpi
{
    private static final Logger logger = LoggerFactory.getLogger(TDClient.class);

    private final TDClientConfig config;
    private final JettyHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public TDClient()
            throws IOException, TDClientException
    {
        this(TDClientConfig.currentConfig());
    }

    public TDClient(TDClientConfig config)
    {
        this.config = config;
        this.httpClient = new JettyHttpClient();
        this.objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public void close() {
        httpClient.close();
    }

    protected ContentResponse submitRequest(Request request)
            throws TDClientException
    {
        ExponentialBackOffRetry retry = new ExponentialBackOffRetry(config.getRetryLimit(), config.getRetryInitialWaitMillis(), config.getRetryWaitMillis());
        Optional<Exception> rootCause = Optional.absent();
        try {
            while (retry.isRunnable()) {
                try {
                    ContentResponse response = request.send();
                    int code =response.getStatus();
                    if(HttpStatus.isSuccess(code)) {
                        // 2xx success
                        return response;
                    }
                    else if(HttpStatus.isClientError(code)) {
                        // 4xx errors
                        throw new TDClientException(ErrorCode.API_CLIENT_ERROR, response.getReason());
                    }
                    else if(HttpStatus.isServerError(code)) {
                        // 5xx errors
                        logger.warn("API request to %s failed with %d: %s", request.getPath(), code, response.getReason());
                    }
                    else {
                        logger.warn("API request to %s failed with code %d: %s", request.getPath(), code, response.getReason());
                    }
                }
                catch(ExecutionException e) {
                    rootCause = Optional.<Exception>of(e);
                    logger.warn("API request failed", e);
                }
                catch(TimeoutException e) {
                    rootCause = Optional.<Exception>of(e);
                    logger.warn(String.format("API request to %s timed out", request.getPath()), e);
                }
                Thread.sleep(retry.nextWaitTimeMillis());
                logger.warn(String.format("Retrying request to %s (%d/%d)", request.getPath(), retry.getRetryCount(), retry.getMaxRetryCount()));
            }
        }
        catch(InterruptedException e) {
            throw new TDClientException(ErrorCode.API_EXECUTION_INTERRUPTED, e);
        }
        throw new TDClientException(ErrorCode.API_RETRY_LIMIT_EXCEEDED, String.format("Failed to process API request to %s", request.getPath()), rootCause);
    }

    protected <Result> Result submit(Request request, Class<Result> resultType)
            throws TDClientException
    {
        try {
            ContentResponse response = submitRequest(request);
            logger.debug("response json:\n" + response.getContentAsString());
            return objectMapper.readValue(response.getContent(), resultType);
        }
        catch (IOException e) {
            throw new TDClientException(ErrorCode.API_INVALID_JSON_RESPONSE, e);
        }
    }

    @Override
    public List<String> listDatabases()
            throws TDClientException
    {
        TDDatabaseList result = submit(prepareGet(TD_API_LIST_DATABASES), TDDatabaseList.class);
        List<String> tableList = new ArrayList<String>(result.getDatabases().size());
        for(TDDatabase db : result.getDatabases()) {
            tableList.add(db.getName());
        }
        return tableList;
    }

    @Override
    public boolean createDatabase(String databaseName)
            throws TDClientException
    {

        return false;
    }

    @Override
    public void deleteDatabase(String databaseName)
            throws TDClientException
    {

    }

    @Override
    public List<TDTable> listTables(String databaseName)
            throws TDClientException
    {
        TDTableList tableList = submit(prepareGet(TD_API_LIST_TABLES + "/" + databaseName), TDTableList.class);
        return tableList.getTables();
    }

    @Override
    public List<TDTable> listTables(TDDatabase database)
            throws TDClientException
    {
        return null;
    }

    @Override
    public TDTable createTable(String databaseName, String tableName)
            throws TDClientException
    {
        return null;
    }

    @Override
    public TDTable createTable(String databaseName, TDTable table)
            throws TDClientException
    {
        return null;
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newTableName)
            throws TDClientException
    {

    }

    @Override
    public void renameTable(TDTable table, String newTableName)
            throws TDClientException
    {

    }

    @Override
    public void deleteTable(String databasename, String tableName)
            throws TDClientException
    {

    }

    @Override
    public void deleteTable(TDTable table)
            throws TDClientException
    {

    }

    @Override
    public void partialDelete(TDTable table, long from, long to)
            throws TDClientException
    {

    }

    @Override
    public Future<TDJobResult> submit(TDJobRequest jobRequest)
            throws TDClientException
    {
        return null;
    }

    @Override
    public List<TDJob> listJobs()
            throws TDClientException
    {
        return null;
    }

    @Override
    public List<TDJob> listJobs(long from, long to)
            throws TDClientException
    {
        return null;
    }

    @Override
    public void killJob(String jobId)
            throws TDClientException
    {

    }

    @Override
    public TDJob jobStatus(String jobId)
            throws TDClientException
    {
        return null;
    }

    @Override
    public TDJobResult jobResult(String jobId)
            throws TDClientException
    {
        return null;
    }

    protected Request prepareGet(String path)
    {
        return prepareRequest(HttpMethod.GET, path, null, null);
    }

    private static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    protected Request prepareRequest(HttpMethod method, String path, Map<String, String> headers, Map<String, String> query)
    {
        String queryString = null;
        if (method == HttpMethod.GET && query != null && !query.isEmpty()) {
            StringBuilder qs = new StringBuilder();
            for (Map.Entry<String, String> entry : query.entrySet()) {
                qs.append(urlEncode(entry.getKey()));
                qs.append("=");
                qs.append(urlEncode(entry.getValue()));
            }
            queryString = qs.toString();
            path = String.format("%s?%s", path, qs);
        }

        Request request = httpClient.newRequest("http://" + config.getEndpoint() + path);
        request.method(method);
        request.agent("TDClient 0.6");
        request.header("Authorization", "TD1 " + config.getApiKey());
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request.header(entry.getKey(), entry.getValue());
            }
        }
        String dateHeader = setDateHeader(request);
        if (method != HttpMethod.GET && queryString != null) {
            request.content(new StringContentProvider(queryString), "application/x-www-form-urlencoded");
        }
        return request;
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

    static String setDateHeader(Request request)
    {
        Date currentDate = new Date();
        String dateHeader = RFC2822_FORMAT.get().format(currentDate);
        request.header("Date", dateHeader);
        return dateHeader;
    }

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

}
