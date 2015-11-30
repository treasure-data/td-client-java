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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.model.TDAuthenticationResult;
import com.treasuredata.client.model.TDBulkImportParts;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDBulkImportSessionList;
import com.treasuredata.client.model.TDDatabase;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSubmitResult;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDPartialDeleteJob;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDTable;
import com.treasuredata.client.model.TDTableList;
import com.treasuredata.client.model.TDTableType;
import com.treasuredata.client.model.TDUpdateTableResult;
import com.treasuredata.client.model.impl.TDDatabaseList;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.treasuredata.client.TDApiRequest.urlEncode;

/**
 *
 */
public class TDClient
        implements TDClientApi<TDClient>
{
    private static final Logger logger = LoggerFactory.getLogger(TDClient.class);
    private static final String version;

    public static String getVersion()
    {
        return version;
    }

    static String readMavenVersion(URL mavenProperties)
    {
        String v = "unknown";
        if (mavenProperties != null) {
            try (InputStream in = mavenProperties.openStream()) {
                Properties p = new Properties();
                p.load(in);
                v = p.getProperty("version", "unknown");
            }
            catch (Throwable e) {
                logger.warn("Error in reading pom.properties file", e);
            }
        }
        return v;
    }

    static {
        URL mavenProperties = TDClient.class.getResource("META-INF/com.treasuredata.client.td-client/pom.properties");
        version = readMavenVersion(mavenProperties);
        logger.info("td-client version: " + version);
    }

    public static TDClient newClient()
    {
        return new TDClientBuilder(true).build();
    }

    public static TDClientBuilder newBuilder()
    {
        return new TDClientBuilder(true);
    }

    /**
     * Create a new TDClient that uses the given api key for the authentication.
     * The new instance of TDClient shares the same HttpClient, so closing this will invalidate the other copy of TDClient instances
     *
     * @param newApiKey
     * @return
     */
    @Override
    public TDClient withApiKey(String newApiKey)
    {
        return new TDClient(config, httpClient, Optional.of(newApiKey));
    }

    @VisibleForTesting
    final TDClientConfig config;
    @VisibleForTesting
    final TDHttpClient httpClient;
    private final Optional<String> apiKeyCache;

    public TDClient(TDClientConfig config)
    {
        this(config, new TDHttpClient(config), config.apiKey);
    }

    protected TDClient(TDClientConfig config, TDHttpClient httpClient, Optional<String> apiKeyCache)
    {
        this.config = config;
        this.httpClient = httpClient;
        this.apiKeyCache = apiKeyCache;
    }

    public void close()
    {
        httpClient.close();
    }

    private static String buildUrl(String urlPrefix, String... args)
    {
        StringBuilder s = new StringBuilder();
        s.append(urlPrefix);
        for (String a : args) {
            s.append("/");
            s.append(urlEncode(a));
        }
        return s.toString();
    }

    private <ResultType> ResultType doGet(String path, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        checkNotNull(path, "path is null");
        checkNotNull(resultTypeClass, "resultTypeClass is null");

        TDApiRequest request = TDApiRequest.Builder.GET(path).build();
        return httpClient.call(request, apiKeyCache, resultTypeClass);
    }

    private <ResultType> ResultType doPost(String path, Map<String, String> queryParam, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        checkNotNull(path, "path is null");
        checkNotNull(queryParam, "param is null");
        checkNotNull(resultTypeClass, "resultTypeClass is null");

        TDApiRequest.Builder request = TDApiRequest.Builder.POST(path);
        for (Map.Entry<String, String> e : queryParam.entrySet()) {
            request.addQueryParam(e.getKey(), e.getValue());
        }
        return httpClient.call(request.build(), apiKeyCache, resultTypeClass);
    }

    private String doPost(String path)
            throws TDClientException
    {
        checkNotNull(path, "path is null");

        TDApiRequest request = TDApiRequest.Builder.POST(path).build();
        return httpClient.call(request, apiKeyCache);
    }

    private String doPut(String path, File filePath)
            throws TDClientException
    {
        checkNotNull(path, "path is null");
        checkNotNull(filePath, "filePath is null");

        TDApiRequest request = TDApiRequest.Builder.PUT(path).setFile(filePath).build();
        return httpClient.call(request, apiKeyCache);
    }

    @Override
    public TDClient authenticate(String email, String password)
    {
        TDAuthenticationResult authResult = doPost("/v3/user/authenticate", ImmutableMap.of("user", email, "password", password), TDAuthenticationResult.class);
        return withApiKey(authResult.getApikey());
    }

    @Override
    public String serverStatus()
    {
        // No API key is requried for server_status
        return httpClient.call(TDApiRequest.Builder.GET("/v3/system/server_status").build(), Optional.<String>absent());
    }

    @Override
    public List<String> listDatabaseNames()
            throws TDClientException
    {
        ArrayList<String> tableList = new ArrayList<>();
        for (TDDatabase db : listDatabases()) {
            tableList.add(db.getName());
        }
        return tableList;
    }

    @Override
    public List<TDDatabase> listDatabases()
            throws TDClientException
    {
        TDDatabaseList result = doGet("/v3/database/list", TDDatabaseList.class);
        return result.getDatabases();
    }

    private static Pattern acceptableNamePattern = Pattern.compile("^([a-z0-9_]+)$");

    static String validateDatabaseName(String databaseName)
    {
        return validateName(databaseName, "Database");
    }

    static String validateTableName(String tableName)
    {
        return validateName(tableName, "Table");
    }

    private static String validateName(String name, String type)
    {
        // Validate database name
        if (name.length() < 3 || name.length() > 256) {
            throw new TDClientException(
                    TDClientException.ErrorType.INVALID_INPUT, String.format(
                    "%s name must be 3 to 256 characters but got %d characters: %s", type, name.length(), name));
        }

        if (!acceptableNamePattern.matcher(name).matches()) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT,
                    String.format("%s name must follow this pattern %s: %s", type, acceptableNamePattern.pattern(), name));
        }
        return name;
    }

    @Override
    public void createDatabase(String databaseName)
            throws TDClientException
    {
        doPost(buildUrl("/v3/database/create", validateDatabaseName(databaseName)));
    }

    @Override
    public void createDatabaseIfNotExists(String databaseName)
            throws TDClientException
    {
        try {
            createDatabase(databaseName);
        }
        catch (TDClientHttpConflictException e) {
            // This can be thrown when the database already exists or Nginx returns conflict(409) upon request retry
        }
    }

    @Override
    public void deleteDatabase(String databaseName)
            throws TDClientException
    {
        doPost(buildUrl("/v3/database/delete", validateDatabaseName(databaseName)));
    }

    @Override
    public void deleteDatabaseIfExists(String databaseName)
            throws TDClientException
    {
        try {
            deleteDatabase(databaseName);
        }
        catch (TDClientHttpNotFoundException e) {
            // This will be thrown when the database does not exist, or Nginx calls this delete request twice
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
        return listDatabaseNames().contains(databaseName);
    }

    @Override
    public boolean existsTable(String databaseName, String tableName)
            throws TDClientException
    {
        try {
            for (TDTable table : listTables(databaseName)) {
                if (table.getName().equals(tableName)) {
                    return true;
                }
            }
            return false;
        }
        catch (TDClientHttpException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND_404) {
                return false;
            }
            else {
                throw e;
            }
        }
    }

    @Override
    public void createTable(String databaseName, String tableName)
            throws TDClientException
    {
        doPost(buildUrl("/v3/table/create", databaseName, validateTableName(tableName), TDTableType.LOG.getTypeName()));
    }

    @Override
    public void createTableIfNotExists(String databaseName, String tableName)
            throws TDClientException
    {
        try {
            createTable(databaseName, tableName);
        }
        catch (TDClientHttpConflictException e) {
            // This can be thrown when the table already exists or Nginx returns conflict(409) upon request retry
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
        doPost(buildUrl("/v3/table/rename", databaseName, tableName, validateTableName(newTableName)),
                ImmutableMap.of("overwrite", Boolean.toString(overwrite)),
                TDUpdateTableResult.class
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
        try {
            deleteTable(databaseName, tableName);
        }
        catch (TDClientHttpNotFoundException e) {
            // This will be thrown the table does not exists or Nginx calls this API request twice
        }
    }

    @Override
    public TDPartialDeleteJob partialDelete(String databaseName, String tableName, long from, long to)
            throws TDClientException
    {
        if ((from % 3600 != 0) || (to % 3600 != 0)) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, String.format("from/to value must be a multiple of 3600: [%s, %s)", from, to));
        }

        Map<String, String> queryParams = ImmutableMap.of(
                "from", Long.toString(from),
                "to", Long.toString(to));
        TDPartialDeleteJob job = doPost(buildUrl("/v3/table/partialdelete", databaseName, tableName), queryParams, TDPartialDeleteJob.class);
        return job;
    }

    @Override
    public void swapTables(String databaseName, String tableName1, String tableName2)
    {
        doPost(buildUrl("/v3/table/swap", databaseName, tableName1, tableName2));
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
        if (jobRequest.getPoolName().isPresent()) {
            queryParam.put("pool_name", jobRequest.getPoolName().get());
        }

        if (logger.isDebugEnabled()) {
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
    public TDJobList listJobs(long fromJobId, long toJobId)
            throws TDClientException
    {
        return doGet(String.format("/v3/job/list?from_id=%d&to_id=%d", fromJobId, toJobId), TDJobList.class);
    }

    @Override
    public void killJob(String jobId)
            throws TDClientException
    {
        doPost(buildUrl("/v3/job/kill", jobId));
    }

    @Override
    public TDJobSummary jobStatus(String jobId)
            throws TDClientException
    {
        return doGet(buildUrl("/v3/job/status", jobId), TDJobSummary.class);
    }

    @Override
    public TDJob jobInfo(String jobId)
            throws TDClientException
    {
        return doGet(buildUrl("/v3/job/show", jobId), TDJob.class);
    }

    @Override
    public <Result> Result jobResult(String jobId, TDResultFormat format, Function<InputStream, Result> resultStreamHandler)
            throws TDClientException
    {
        TDApiRequest request = TDApiRequest.Builder
                .GET(buildUrl("/v3/job/result", jobId))
                .addQueryParam("format", format.getName())
                .build();
        return httpClient.<Result>call(request, apiKeyCache, resultStreamHandler);
    }

    @Override
    public List<TDBulkImportSession> listBulkImportSessions()
    {
        return doGet(buildUrl("/v3/bulk_import/list"), TDBulkImportSessionList.class).getSessions();
    }

    @Override
    public List<String> listBulkImportParts(String sessionName)
    {
        return doGet(buildUrl("/v3/bulk_import/list_parts", sessionName), TDBulkImportParts.class).getParts();
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
        doPut(buildUrl("/v3/bulk_import/upload_part", sessionName, uniquePartName), path);
    }

    public void deleteBulkImportPart(String sessionName, String uniquePartName)
    {
        doPost(buildUrl("/v3/bulk_import/delete_part", sessionName, uniquePartName));
    }

    @Override
    public void freezeBulkImportSession(String sessionName)
    {
        doPost(buildUrl("/v3/bulk_import/freeze", sessionName));
    }

    @Override
    public void unfreezeBulkImportSession(String sessionName)
    {
        doPost(buildUrl("/v3/bulk_import/unfreeze", sessionName));
    }

    @Override
    public void performBulkImportSession(String sessionName)
    {
        doPost(buildUrl("/v3/bulk_import/perform", sessionName));
    }

    @Override
    public void commitBulkImportSession(String sessionName)
    {
        doPost(buildUrl("/v3/bulk_import/commit", sessionName));
    }

    @Override
    public void deleteBulkImportSession(String sessionName)
    {
        doPost(buildUrl("/v3/bulk_import/delete", sessionName));
    }

    @Override
    public <Result> Result getBulkImportErrorRecords(String sessionName, Function<InputStream, Result> resultStreamHandler)
    {
        TDApiRequest request = TDApiRequest.Builder
                .GET(buildUrl("/v3/bulk_import/error_records", sessionName))
                .build();
        return httpClient.<Result>call(request, apiKeyCache, resultStreamHandler);
    }
}
