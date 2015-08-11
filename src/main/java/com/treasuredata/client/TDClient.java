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

import com.treasuredata.client.api.ApiRequest;
import com.treasuredata.client.api.TDHttpClient;
import com.treasuredata.client.api.model.TDDatabase;
import com.treasuredata.client.api.model.TDDatabaseList;
import com.treasuredata.client.api.model.TDJob;
import com.treasuredata.client.api.model.TDJobList;
import com.treasuredata.client.api.model.TDJobRequest;
import com.treasuredata.client.api.model.TDJobResult;
import com.treasuredata.client.api.model.TDJobStatus;
import com.treasuredata.client.api.model.TDTable;
import com.treasuredata.client.api.model.TDTableList;
import org.eclipse.jetty.client.api.ContentResponse;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.treasuredata.client.api.ApiRequest.urlEncode;

/**
 *
 */
public class TDClient
        implements TDClientSpi
{
    private static final Logger logger = LoggerFactory.getLogger(TDClient.class);
    private final TDClientConfig config;
    private final TDHttpClient httpClient;

    public TDClient()
            throws IOException, TDClientException
    {
        this(TDClientConfig.currentConfig());
    }

    public TDClient(TDClientConfig config)
    {
        this.config = config;
        this.httpClient = new TDHttpClient(config);
    }

    public void close()
    {
        httpClient.close();
    }

    private <ResultType> ResultType doGet(String path, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        ApiRequest request = ApiRequest.Builder.GET(path).build();
        return httpClient.submit(request, resultTypeClass);
    }

    private <ResultType> ResultType doPost(String path, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        ApiRequest request = ApiRequest.Builder.GET(path).build();
        return httpClient.submit(request, resultTypeClass);
    }

    private ContentResponse doPost(String path)
            throws TDClientException
    {
        ApiRequest request = ApiRequest.Builder.GET(path).build();
        return httpClient.submit(request);
    }



    @Override
    public List<String> listDatabases()
            throws TDClientException
    {
        TDDatabaseList result = doGet("/v3/database/list", TDDatabaseList.class);
        List<String> tableList = new ArrayList<String>(result.getDatabases().size());
        for (TDDatabase db : result.getDatabases()) {
            tableList.add(db.getName());
        }
        return tableList;
    }

    @Override
    public boolean createDatabase(String databaseName)
            throws TDClientException
    {
        doPost(String.format("/v3/database/create/%s", urlEncode(databaseName)));
        return true;
    }

    @Override
    public void deleteDatabase(String databaseName)
            throws TDClientException
    {
        doPost(String.format("/v3/database/delete/%s", urlEncode(databaseName)));
    }

    @Override
    public List<TDTable> listTables(String databaseName)
            throws TDClientException
    {
        TDTableList tableList = doGet("/v3/table/list/" + urlEncode(databaseName), TDTableList.class);
        return tableList.getTables();
    }

    @Override
    public List<TDTable> listTables(TDDatabase database)
            throws TDClientException
    {
        return listTables(database.getName());
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
    public TDJobStatus submit(TDJobRequest jobRequest)
            throws TDClientException
    {
        ApiRequest.Builder request = ApiRequest.Builder.POST(
                String.format("/v3/job/issue/%s/%s",
                        urlEncode(jobRequest.getType().getType()),
                        urlEncode(jobRequest.getDatabase())));

        request.addQueryParam("query", jobRequest.getQuery());
        request.addQueryParam("version", getVersion());
        if (jobRequest.getResultOutput().isPresent()) {
            request.addQueryParam("result", jobRequest.getResultOutput().get());
        }
        request.addQueryParam("priority", Integer.toString(jobRequest.getPriority().toInt()));
        request.addQueryParam("retry_limit", Integer.toString(jobRequest.getRetryLimit()));

        return httpClient.submit(request.build(), TDJobStatus.class);
    }

    @Override
    public TDJobList listJobs()
            throws TDClientException
    {
        return doGet("/v3/job/list", TDJobList.class);
    }

    @Override
    public TDJobList listJobs(long from, long to)
            throws TDClientException
    {
        return doGet(String.format("/v3/job/list?from=%d&to=%d", from, to), TDJobList.class);
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

    private static final String version;

    public static String getVersion()
    {
        return version;
    }

    static {
        URL mavenProperties = TDClient.class.getResource("META-INF/com.treasuredata.client.td-client/pom.properties");
        String v = "unknown";
        if (mavenProperties != null) {
            InputStream in = null;
            try {
                try {
                    in = mavenProperties.openStream();
                    Properties p = new Properties();
                    p.load(in);
                    v = p.getProperty("version", "unknown");
                }
                finally {
                    if (in != null) {
                        in.close();
                    }
                }
            }
            catch (Throwable e) {
                logger.warn("Error in reading pom.properties file", e);
            }
        }
        version = v;
    }
}
