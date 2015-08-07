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

//import com.google.common.collect.ImmutableList;
//import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.treasuredata.client.api.model.TDDatabase;
import com.treasuredata.client.api.model.TDDatabaseList;
import com.treasuredata.client.api.model.TDTable;
import com.treasuredata.client.api.model.TDTableList;
import com.treasuredata.client.api.model.TDTableSchema;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.HttpCookieStore;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

// Jetty 9 requires Java 7

//import javax.annotation.PostConstruct;
//import javax.annotation.PreDestroy;
//import javax.inject.Inject;

public class TDApiClient
        implements Closeable
{
    private final TDApiClientConfig config;
    private final HttpClient http;
    private final ObjectMapper objectMapper;

    //@Inject
    public TDApiClient(TDApiClientConfig config)
    {
        this.config = config;

        this.objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        this.http = new HttpClient();
        http.setConnectTimeout(10 * 1000);
        http.setIdleTimeout(60 * 1000);
        http.setMaxConnectionsPerDestination(16);
        http.setCookieStore(new HttpCookieStore.Empty());

        try {
            http.start();
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed to start http client", ex);
        }
    }

    //@PreDestroy
    public void close()
    {
        try {
            http.stop();
        }
        catch (Exception ex) {
            throw new RuntimeException("failed to stop http client", ex);
        }
    }

    public List<TDDatabase> getDatabases(String apikey)
    {
        Request request = prepareExchange(apikey, HttpMethod.GET, buildUrl("/v3/database/list"));

        ContentResponse response = executeExchange(request);

        TDDatabaseList databaseList = parseResponse(response.getContent(), TDDatabaseList.class);

        return databaseList.getDatabases();
    }

    public List<TDTable> getTables(String apikey, String databaseName)
    {
        Request request = prepareExchange(apikey, HttpMethod.GET, buildUrl("/v3/table/list", databaseName));

        ContentResponse response = executeExchange(request);

        TDTableList tables = parseResponse(response.getContent(), TDTableList.class);

        return tables.getTables();
    }

    public void bulkloadStarted(String apikey, int jobId, long startAt)
    {
        jobStarted(apikey, jobId, startAt);
    }

    public void bulkloadFinished(String apikey, int jobId, boolean success, long startAt, long endAt,
            long recordCount, long recordSize, JsonNode configDiff, TDTableSchema schema)
    {
        Map<String, String> params = new HashMap<>();
        params.put("records", Long.toString(recordCount));
        params.put("result_size", Long.toString(recordSize));
        if (configDiff != null) {
            params.put("config_diff", formatRequestParameterObject(configDiff));
        }
        if (schema != null) {
            //[["c2","int","k2"],["c3","int","k3"]]
            params.put("schema", formatRequestParameterObject(schema.getColumns()));
        }
        jobFinished(apikey, jobId, success, startAt, endAt, params);
    }

    public void jobStarted(String apikey, int jobId, long startAt)
    {
        Request request = prepareExchange(apikey, HttpMethod.POST, buildUrl("/v3/job/started", Integer.toString(jobId)));
        ContentProvider provider = new BytesContentProvider("application/x-www-form-urlencoded",
                formatPostRequestContent("start_at", Long.toString(startAt)));
        request.content(provider, "application/x-www-form-urlencoded");
        ContentResponse response = executeExchange(request);
    }

    public void jobFinished(String apikey, int jobId, boolean success, long startAt, long endAt,
            Map<String, String> params)
    {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder();
        builder.add("start_at");
        builder.add(Long.toString(startAt));
        builder.add("end_at");
        builder.add(Long.toString(endAt));
        builder.add("status");
        builder.add(success ? "success" : "error");
        for (Map.Entry<String, String> e : params.entrySet()) {
            builder.add(e.getKey());
            builder.add(e.getValue());
        }
        ImmutableList<String> kvs = builder.build();

        Request request = prepareExchange(apikey, HttpMethod.POST, buildUrl("/v3/job/finished", Integer.toString(jobId)));
        ContentProvider provider = new BytesContentProvider("application/x-www-form-urlencoded",
                formatPostRequestContent(kvs.toArray(new String[kvs.size()])));
        request.content(provider, "application/x-www-form-urlencoded");
        ContentResponse response = executeExchange(request);
    }

    public void mergeTableSchema(String apikey, int tableId, TDTableSchema schema)
    {
        Request request = prepareExchange(apikey, HttpMethod.POST, buildUrl("/v3/table/merge_schema", Integer.toString(tableId)));

        // schema=[["col1","int"]], urlencoded
        String schemaParameter = formatRequestParameterObject(schema.getColumns());

        ContentProvider provider = new BytesContentProvider("application/x-www-form-urlencoded",
                formatPostRequestContent("schema", schemaParameter));
        request.content(provider, "application/x-www-form-urlencoded");

        ContentResponse response = executeExchange(request);

        TDSchemaMergeResponse merged = parseResponse(response.getContent(), TDSchemaMergeResponse.class);

        //return merged.getParsedSchema(objectMapper);
    }

    private static class TDSchemaMergeResponse
    {
        private final String schema;

        @JsonCreator
        public TDSchemaMergeResponse(@JsonProperty("schema") String schema)
        {
            this.schema = schema;
        }

        @JsonProperty
        public String getSchema()
        {
            return schema;
        }

        //public List<TDColumn> getParsedSchema(ObjectMapper objectMapper)
        //{
        //    try {
        //        return objectMapper.readValue(schema, List<TDColumn>.class);  // doesn't work
        //    } catch (IOException ex) {
        //        throw new RuntimeException(ex);
        //    }
        //}
    }

    private Request prepareExchange(String apikey, HttpMethod method, String url)
    {
        return prepareExchange(apikey, method, url, Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap());
    }

    private Request prepareExchange(String apikey, HttpMethod method, String url, Map<String, String> headers, Map<String, String> query)
    {
        String queryString = null;
        if (!query.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : query.entrySet()) {
                try {
                    sb.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                    sb.append("=");
                    sb.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
                }
                catch (UnsupportedEncodingException ex) {
                    throw new AssertionError(ex);
                }
            }
            queryString = sb.toString();
        }

        if (method == HttpMethod.GET && queryString != null) {
            url = url + "?" + queryString;
        }
        Request request = http.newRequest(url);
        request.method(method);
        request.agent("TDApiClient v003");
        request.header("Authorization", "TD1 " + apikey);
        //request.timeout(60, TimeUnit.SECONDS);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.header(entry.getKey(), entry.getValue());
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

    private static String setDateHeader(Request request)
    {
        Date currentDate = new Date();
        String dateHeader = RFC2822_FORMAT.get().format(currentDate);
        request.header("Date", dateHeader);
        return dateHeader;
    }


    private String buildUrl(String path, String... params)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(config.getEndpoint());
        sb.append(path);
        try {
            for (String param : params) {
                sb.append("/");
                sb.append(URLEncoder.encode(param, "UTF-8"));
            }
        }
        catch (UnsupportedEncodingException ex) {
            throw new AssertionError(ex);
        }
        return sb.toString();
    }

    private ContentResponse executeExchange(Request request)
    {
        int retryLimit = 5;
        int retryWait = 500;

        Exception firstException = null;

        try {
            while (true) {
                Exception exception;

                try {
                    ContentResponse response = request.send();

                    int status = response.getStatus();
                    switch (status) {
                        case 200:
                            return response;
                        case 404:
                            throw new TDApiNotFoundException(status, response.getContent());
                        case 409:
                            throw new TDApiConflictException(status, response.getContent());
                    }

                    if (status / 100 != 5) {  // not 50x
                        throw new TDApiResponseException(status, response.getContent());
                    }

                    // retry on 50x and other errors
                    exception = new TDApiResponseException(status, response.getContent());
                }
                catch (TDApiException ex) {
                    throw ex;
                }
                catch (Exception ex) {
                    // retry on RuntimeException
                    exception = ex;
                }

                if (firstException == null) {
                    firstException = exception;
                }

                if (retryLimit <= 0) {
                    if (firstException instanceof TDApiException) {
                        throw (TDApiException) firstException;
                    }
                    throw new TDApiExecutionException(firstException);
                }

                retryLimit -= 1;
                Thread.sleep(retryWait);
                retryWait *= 2;
            }
        }
        catch (InterruptedException ex) {
            throw new TDApiExecutionInterruptedException(ex);
        }
    }

    private String formatRequestParameterObject(Object obj)
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        try {
            objectMapper.writeValue(bo, obj);
            return new String(bo.toByteArray(), "UTF-8");
        }
        catch (UnsupportedEncodingException ex) {
            throw new AssertionError(ex);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            try {
                bo.close();
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private byte[] formatPostRequestContent(String... kvs)
    {
        try {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < kvs.length; i += 2) {
                if (i > 0) {
                    sb.append("&");
                }
                sb.append(URLEncoder.encode(kvs[i], "UTF-8"));
                sb.append("=");
                sb.append(URLEncoder.encode(kvs[i + 1], "UTF-8"));
            }
            return sb.toString().getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException ex) {
            throw new AssertionError(ex);
        }
    }

    private <T> T parseResponse(byte[] content, Class<T> valueType)
    {
        try {
            return objectMapper.readValue(content, valueType);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
