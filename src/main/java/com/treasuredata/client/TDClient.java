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

import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.api.TDApiRequest;
import com.treasuredata.client.api.model.ResultFormat;
import com.treasuredata.client.api.model.TDBulkImportSession;
import com.treasuredata.client.api.model.TDDatabase;
import com.treasuredata.client.api.model.TDDatabaseList;
import com.treasuredata.client.api.model.TDJob;
import com.treasuredata.client.api.model.TDJobList;
import com.treasuredata.client.api.model.TDJobRequest;
import com.treasuredata.client.api.model.TDJobStatus;
import com.treasuredata.client.api.model.TDJobSubmitResult;
import com.treasuredata.client.api.model.TDTable;
import com.treasuredata.client.api.model.TDTableList;
import com.treasuredata.client.api.model.TDTableType;
import com.treasuredata.client.api.model.UpdateTableResult;
import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.treasuredata.client.api.TDApiRequest.urlEncode;

/**
 *
 */
public class TDClient
        implements TDClientApi
{
    private static final Logger logger = LoggerFactory.getLogger(TDClient.class);
    private static final String version;

    public static String getVersion()
    {
        return version;
    }

    static {
        URL mavenProperties = TDClient.class.getResource("META-INF/com.treasuredata.client.td-client/pom.properties");
        String v = "unknown";
        if (mavenProperties != null) {
            try(InputStream in = mavenProperties.openStream()) {
                Properties p = new Properties();
                p.load(in);
                v = p.getProperty("version", "unknown");
            }
            catch (Throwable e) {
                logger.warn("Error in reading pom.properties file", e);
            }
        }
        version = v;
        logger.info("td-client version: " + version);
    }


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

    private static String buildUrl(String urlPrefix, String... args)
    {
        StringBuilder s = new StringBuilder();
        s.append(urlPrefix);
        for(String a : args) {
            s.append("/");
            s.append(urlEncode(a));
        }
        return s.toString();
    }

    private <ResultType> ResultType doGet(String path, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        TDApiRequest request = TDApiRequest.Builder.GET(path).build();
        return httpClient.submit(request, resultTypeClass);
    }

    private <ResultType> ResultType doPost(String path, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        return doPost(path, ImmutableMap.<String, String>of(), resultTypeClass);
    }

    private <ResultType> ResultType doPost(String path, Map<String, String> queryParam, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        checkNotNull(path, "pash is null");
        checkNotNull(queryParam, "param is null");
        checkNotNull(resultTypeClass, "resultTypeClass is null");

        TDApiRequest.Builder request = TDApiRequest.Builder.POST(path);
        for (Map.Entry<String, String> e : queryParam.entrySet()) {
            request.addQueryParam(e.getKey(), e.getValue());
        }
        return httpClient.submit(request.build(), resultTypeClass);
    }

    private ContentResponse doPost(String path, Map<String, String> queryParam)
            throws TDClientException
    {
        checkNotNull(path, "path is null");
        checkNotNull(queryParam, "param is null");

        TDApiRequest.Builder request = TDApiRequest.Builder.POST(path);
        for (Map.Entry<String, String> e : queryParam.entrySet()) {
            request.addQueryParam(e.getKey(), e.getValue());
        }
        return httpClient.submit(request.build());
    }


    private ContentResponse doPost(String path)
            throws TDClientException
    {
        TDApiRequest request = TDApiRequest.Builder.POST(path).build();
        return httpClient.submit(request);
    }

    private ContentResponse doPut(String path, File filePath)
            throws TDClientException
    {
        TDApiRequest request = TDApiRequest.Builder.PUT(path).setFile(filePath).build();
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
    public void createDatabase(String databaseName)
            throws TDClientException
    {
        doPost(buildUrl("/v3/database/create", databaseName));
    }

    @Override
    public void createDatabaseIfNotExists(String databaseName)
            throws TDClientException
    {
        if(!existsDatabase(databaseName)) {
            createDatabase(databaseName);
        }
    }

    @Override
    public void deleteDatabase(String databaseName)
            throws TDClientException
    {
        doPost(buildUrl("/v3/database/delete", databaseName));
    }

    @Override
    public void deleteDatabaseIfExists(String databaseName)
            throws TDClientException
    {
        if(existsDatabase(databaseName)) {
            deleteDatabase(databaseName);
        }
    }

    @Override
    public List<TDTable> listTables(String databaseName)
            throws TDClientException
    {
        TDTableList tableList = doGet(buildUrl("/v3/table/list", databaseName), TDTableList.class);
        return tableList.getTables();
    }

    @Override
    public boolean existsDatabase(String databaseName)
            throws TDClientException
    {
        return listDatabases().contains(databaseName);
    }

    @Override
    public boolean existsTable(String databaseName, String tableName)
            throws TDClientException
    {
        for(TDTable table : listTables(databaseName)) {
            if(table.getName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void createTable(String databaseName, String tableName)
            throws TDClientException
    {
        doPost(buildUrl("/v3/table/create", databaseName, tableName, TDTableType.LOG.getTypeName()));
    }

    @Override
    public void createTableIfNotExists(String databaseName, String tableName)
            throws TDClientException
    {
        if(!existsTable(databaseName, tableName)) {
            createTable(databaseName, tableName);
        }
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newTableName)
            throws TDClientException
    {
        renameTable(databaseName, tableName, newTableName, false);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newTableName, boolean overwrite)
            throws TDClientException
    {
        doPost(buildUrl("/v3/table/rename", databaseName, tableName, newTableName),
                ImmutableMap.of("overwrite", Boolean.toString(overwrite)),
                UpdateTableResult.class
        );
    }

    @Override
    public void deleteTable(String databaseName, String tableName)
            throws TDClientException
    {
        doPost(buildUrl("/v3/table/delete", databaseName, tableName));
    }

    @Override
    public void deleteTableIfExists(String databaseName, String tableName)
            throws TDClientException
    {
        if(existsTable(databaseName, tableName)) {
            deleteTable(databaseName, tableName);
        }
    }

    @Override
    public void partialDelete(String databaseName, String tableName, long from, long to)
            throws TDClientException
    {
        Map<String, String> queryParams = ImmutableMap.of(
                "from", Long.toString(from),
                "to", Long.toString(to));
        doPost(buildUrl("/v3/table/partialdelete", databaseName, tableName), queryParams);
    }

    @Override
    public String submit(TDJobRequest jobRequest)
            throws TDClientException
    {
        Map<String, String> queryParam = new HashMap<>();
        queryParam.put("query", jobRequest.getQuery());
        queryParam.put("version", getVersion());
        if (jobRequest.getResultOutput().isPresent()) {
            queryParam.put("result", jobRequest.getResultOutput().get());
        }
        queryParam.put("priority", Integer.toString(jobRequest.getPriority().toInt()));
        queryParam.put("retry_limit", Integer.toString(jobRequest.getRetryLimit()));

        if(logger.isDebugEnabled()) {
            logger.debug("submit job: " + jobRequest);
        }

        TDJobSubmitResult result =
                doPost(
                        buildUrl("/v3/job/issue", jobRequest.getType().getType(), jobRequest.getDatabase()),
                        queryParam,
                        TDJobSubmitResult.class);
        return result.getJobId();
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
        doPost(buildUrl("/v3/job/kill", jobId));
    }

    @Override
    public TDJobStatus jobStatus(String jobId)
            throws TDClientException
    {
        return doGet(buildUrl("/v3/job/status", jobId), TDJobStatus.class);
    }

    @Override
    public TDJob jobInfo(String jobId)
            throws TDClientException
    {
        return doGet(buildUrl("/v3/job/show", jobId), TDJob.class);
    }

    @Override
    public InputStream jobResult(String jobId, ResultFormat format)
            throws TDClientException
    {
        TDApiRequest request = TDApiRequest.Builder
                .GET(buildUrl("/v3/job/result", jobId))
                .addQueryParam("format", format.getName())
                .build();
        return httpClient.openStream(request);
    }

    @Override
    public void createBulkImportSession(String sessionName, String databaseName, String tableName)
    {
        doPost(buildUrl("/v3/bulk_import/create", sessionName, databaseName, tableName));
    }

    @Override
    public TDBulkImportSession getBulkImportSession(String sessionName)
    {
        return doGet(buildUrl("/v3/bulk_import/show", sessionName), TDBulkImportSession.class);
    }

    @Override
    public void uploadBulkImportPart(String sessionName, String uniquePartName, File path)
    {
        doPut(buildUrl("/v3/bulk_import/upload_port", sessionName, uniquePartName), path);
    }

    @Override
    public void freezeBulkImportSession(String sessionName)
    {
        doPost(buildUrl("/v3/bulk_import/freeze", sessionName));
    }

    @Override
    public void performBulkImportSession(String sessionName, int priority)
    {
        doPost(buildUrl("/v3/bulk_import/perform", sessionName));
    }

    @Override
    public void commitBulkImportSession(String sessionName)
    {
        doPost(buildUrl("/v3/builk_import/commit", sessionName));
    }

    @Override
    public void deleteBulkImportSession(String sessionName) {
        doPost(buildUrl("/v3/bulk_import/delete", sessionName));
    }


}
