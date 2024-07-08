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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Multimap;
import com.treasuredata.client.model.ObjectMappers;
import com.treasuredata.client.model.TDApiKey;
import com.treasuredata.client.model.TDAuthenticationResult;
import com.treasuredata.client.model.TDBulkImportParts;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDBulkLoadSessionStartRequest;
import com.treasuredata.client.model.TDBulkLoadSessionStartResult;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDConnectionLookupResult;
import com.treasuredata.client.model.TDDatabase;
import com.treasuredata.client.model.TDExportJobRequest;
import com.treasuredata.client.model.TDExportResultJobRequest;
import com.treasuredata.client.model.TDFederatedQueryConfig;
import com.treasuredata.client.model.TDImportResult;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSubmitResult;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDPartialDeleteJob;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import com.treasuredata.client.model.TDSavedQueryHistory;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import com.treasuredata.client.model.TDSavedQueryStartRequestV4;
import com.treasuredata.client.model.TDSavedQueryStartResultV4;
import com.treasuredata.client.model.TDSavedQueryUpdateRequest;
import com.treasuredata.client.model.TDTable;
import com.treasuredata.client.model.TDTableDistribution;
import com.treasuredata.client.model.TDTableList;
import com.treasuredata.client.model.TDTableType;
import com.treasuredata.client.model.TDUpdateTableResult;
import com.treasuredata.client.model.TDUser;
import com.treasuredata.client.model.TDUserList;
import com.treasuredata.client.model.impl.TDScheduleRunResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

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
        URL mavenProperties = TDClient.class.getResource("/META-INF/maven/com.treasuredata.client/td-client/pom.properties");
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

    public static TDClientBuilder newBuilder(boolean loadTDConf)
    {
        return new TDClientBuilder(loadTDConf);
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

    /**
     * @deprecated Use {@link #withHeaders(Map)} instead.
     * @param headers
     * @return
     */
    @Deprecated
    @Override
    public TDClient withHeaders(Multimap<String, String> headers)
    {
        return new TDClient(config, httpClient.withHeaders(headers), apiKeyCache);
    }

    @Override
    public TDClient withHeaders(Map<String, ? extends Collection<String>> headers)
    {
        return new TDClient(config, httpClient.withHeaders(headers), apiKeyCache);
    }

    /**
     * Visible for testing.
     */
    protected final TDClientConfig config;

    /**
     * Visible for testing.
     */
    protected final TDHttpClient httpClient;

    protected final Optional<String> apiKeyCache;

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

    protected static String buildUrl(String urlPrefix, String... args)
    {
        StringBuilder s = new StringBuilder();
        s.append(urlPrefix);
        for (String a : args) {
            s.append("/");
            s.append(UrlPathSegmentEscaper.escape(a));
        }
        return s.toString();
    }

    protected <ResultType> ResultType doGet(String path, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        requireNonNull(path, "path is null");
        requireNonNull(resultTypeClass, "resultTypeClass is null");

        TDApiRequest request = TDApiRequest.Builder.GET(path).build();
        return httpClient.call(request, apiKeyCache, resultTypeClass);
    }

    protected <ResultType> ResultType doGet(String path, TypeReference<ResultType> resultTypeReference)
            throws TDClientException
    {
        requireNonNull(path, "path is null");
        requireNonNull(resultTypeReference, "resultTypeReference is null");

        TDApiRequest request = TDApiRequest.Builder.GET(path).build();
        return httpClient.call(request, apiKeyCache, resultTypeReference);
    }

    protected <ResultType> ResultType doGet(String path, JavaType resultType)
            throws TDClientException
    {
        requireNonNull(path, "path is null");
        requireNonNull(resultType, "resultType is null");

        TDApiRequest request = TDApiRequest.Builder.GET(path).build();
        return httpClient.call(request, apiKeyCache, resultType);
    }

    protected <ResultType> ResultType doPost(String path, Map<String, String> queryParam, Optional<String> jsonBody, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        requireNonNull(path, "path is null");
        requireNonNull(queryParam, "param is null");
        requireNonNull(jsonBody, "body is null");
        requireNonNull(resultTypeClass, "resultTypeClass is null");

        TDApiRequest.Builder request = TDApiRequest.Builder.POST(path);
        for (Map.Entry<String, String> e : queryParam.entrySet()) {
            request.addQueryParam(e.getKey(), e.getValue());
        }
        if (jsonBody.isPresent()) {
            request.setPostJson(jsonBody.get());
        }
        return httpClient.call(request.build(), apiKeyCache, resultTypeClass);
    }

    protected <ResultType> ResultType doPost(String path, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        return this.<ResultType>doPost(path, Collections.emptyMap(), Optional.empty(), resultTypeClass);
    }

    protected <ResultType> ResultType doPost(String path, Map<String, String> queryParam, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        return this.<ResultType>doPost(path, queryParam, Optional.empty(), resultTypeClass);
    }

    protected <ResultType> ResultType doPut(String path, Map<String, String> queryParam, File file, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        requireNonNull(file, "file is null");
        requireNonNull(resultTypeClass, "resultTypeClass is null");

        TDApiRequest.Builder request = buildPutRequest(path, queryParam);
        request.setFile(file);
        return httpClient.call(request.build(), apiKeyCache, resultTypeClass);
    }

    protected <ResultType> ResultType doPut(String path, Map<String, String> queryParam, byte[] content, int offset, int length, Class<ResultType> resultTypeClass)
            throws TDClientException
    {
        requireNonNull(content, "content is null");
        requireNonNull(resultTypeClass, "resultTypeClass is null");

        TDApiRequest.Builder request = buildPutRequest(path, queryParam);
        request.setContent(content, offset, length);
        return httpClient.call(request.build(), apiKeyCache, resultTypeClass);
    }

    private TDApiRequest.Builder buildPutRequest(String path, Map<String, String> queryParam)
    {
        requireNonNull(path, "path is null");
        requireNonNull(queryParam, "param is null");

        TDApiRequest.Builder request = TDApiRequest.Builder.PUT(path);
        for (Map.Entry<String, String> e : queryParam.entrySet()) {
            request.addQueryParam(e.getKey(), e.getValue());
        }

        return request;
    }

    protected String doPost(String path)
            throws TDClientException
    {
        requireNonNull(path, "path is null");

        TDApiRequest request = TDApiRequest.Builder.POST(path).build();
        return httpClient.call(request, apiKeyCache);
    }

    protected String doPost(String path, Map<String, String> queryParam)
            throws TDClientException
    {
        requireNonNull(path, "path is null");
        requireNonNull(queryParam, "param is null");

        TDApiRequest.Builder request = TDApiRequest.Builder.POST(path);
        for (Map.Entry<String, String> e : queryParam.entrySet()) {
            request.addQueryParam(e.getKey(), e.getValue());
        }

        return httpClient.call(request.build(), apiKeyCache);
    }

    protected String doPut(String path, File filePath)
            throws TDClientException
    {
        requireNonNull(path, "path is null");
        requireNonNull(filePath, "filePath is null");

        TDApiRequest request = TDApiRequest.Builder.PUT(path).setFile(filePath).build();
        return httpClient.call(request, apiKeyCache);
    }

    @Override
    public TDClient authenticate(String email, String password)
    {
        Map<String, String> m = new HashMap<>();
        m.put("user", email);
        m.put("password", password);
        TDAuthenticationResult authResult = doPost("/v3/user/authenticate", Collections.unmodifiableMap(m), TDAuthenticationResult.class);
        return withApiKey(authResult.getApikey());
    }

    @Override
    public TDUser getUser()
    {
        return doGet("/v3/user/show", TDUser.class);
    }

    @Override
    public TDApiKey validateApiKey()
    {
        return doGet("/v3/user/apikey/validate", TDApiKey.class);
    }

    @Override
    public TDUserList listUsers()
    {
        return doGet("/v3/user/list", TDUserList.class);
    }

    @Override
    public String serverStatus()
    {
        // No API key is requried for server_status
        return httpClient.call(TDApiRequest.Builder.GET("/v3/system/server_status").build(), Optional.empty());
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
        return doGet("/v3/database/list", new TypeReference<List<TDDatabase>>() {});
    }

    @Override
    public TDDatabase showDatabase(String databaseName)
            throws TDClientException
    {
        return doGet(buildUrl("/v3/database/show", databaseName), TDDatabase.class);
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
    public TDTable showTable(String databaseName, String tableName)
    {
        return doGet(buildUrl("/v3/table/show", databaseName, tableName), TDTable.class);
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
    public void createTable(String databaseName, String tableName, String idempotentKey)
            throws TDClientException
    {
        // Idempotent key support is EXPERIMENTAL.
        doPost(buildUrl("/v3/table/create", databaseName, validateTableName(tableName), TDTableType.LOG.getTypeName()),
                Collections.singletonMap("idempotent_key", idempotentKey));
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
                Collections.singletonMap("overwrite", Boolean.toString(overwrite)),
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
        return partialDelete(databaseName, tableName, from, to, null);
    }

    @Override
    public TDPartialDeleteJob partialDelete(String databaseName, String tableName, long from, long to, String domainKey)
            throws TDClientException
    {
        if ((from % 3600 != 0) || (to % 3600 != 0)) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, String.format("from/to value must be a multiple of 3600: [%s, %s)", from, to));
        }

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("from", Long.toString(from));
        queryParams.put("to", Long.toString(to));

        if (domainKey != null) {
            queryParams.put("domain_key", domainKey);
        }

        TDPartialDeleteJob job = doPost(buildUrl("/v3/table/partialdelete", databaseName, tableName), Collections.unmodifiableMap(queryParams), TDPartialDeleteJob.class);
        return job;
    }

    @Override
    public void swapTables(String databaseName, String tableName1, String tableName2)
    {
        doPost(buildUrl("/v3/table/swap", databaseName, tableName1, tableName2));
    }

    @Override
    public void updateTableSchema(String databaseName, String tableName, List<TDColumn> newSchema)
    {
        updateTableSchema(databaseName, tableName, newSchema, false);
    }

    @Override
    public void updateTableSchema(String databaseName, String tableName, List<TDColumn> newSchema, boolean ignoreDuplicate)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newSchema, "newSchema is null");

        List<List<String>> builder = new ArrayList<>(newSchema.size());
        for (TDColumn newColumn : newSchema) {
            builder.add(Arrays.asList(newColumn.getKeyString(), newColumn.getType().toString(), newColumn.getName()));
        }
        Map<String, Object> m = new HashMap<>();
        m.put("schema", Collections.unmodifiableList(builder));
        m.put("ignore_duplicate_schema", ignoreDuplicate);
        String schemaJson = toJSONString(m);
        doPost(buildUrl("/v3/table/update-schema", databaseName, tableName), Collections.emptyMap(), Optional.of(schemaJson), String.class);
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static String toJSONString(Map<String, Object> map)
    {
        try {
            return objectMapper.writeValueAsString(map);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendTableSchema(String databaseName, String tableName, List<TDColumn> appendedSchema)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(appendedSchema, "appendedSchema is null");

        List<List<String>> builder = new ArrayList<>(appendedSchema.size());
        for (TDColumn appendedColumn : appendedSchema) {
            // Unlike update-schema API, append-schema API can generate alias for column name.
            // So we should not pass `appendedColumn.getName()` here.
            builder.add(Arrays.asList(appendedColumn.getKeyString(), appendedColumn.getType().toString()));
        }
        String schemaJson = toJSONString(Collections.singletonMap("schema", Collections.unmodifiableList(builder)));
        doPost(buildUrl("/v3/table/append-schema", databaseName, tableName), Collections.emptyMap(), Optional.of(schemaJson), String.class);
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
        if (jobRequest.getRetryLimit().isPresent()) {
            queryParam.put("retry_limit", Integer.toString(jobRequest.getRetryLimit().get()));
        }
        if (jobRequest.getPoolName().isPresent()) {
            queryParam.put("pool_name", jobRequest.getPoolName().get());
        }
        if (jobRequest.getTable().isPresent()) {
            queryParam.put("table", jobRequest.getTable().get());
        }
        if (jobRequest.getScheduledTime().isPresent()) {
            queryParam.put("scheduled_time", String.valueOf(jobRequest.getScheduledTime().get()));
        }
        if (jobRequest.getDomainKey().isPresent()) {
            queryParam.put("domain_key", jobRequest.getDomainKey().get());
        }
        if (jobRequest.getResultConnectionId().isPresent()) {
            queryParam.put("result_connection_id", String.valueOf(jobRequest.getResultConnectionId().get()));
        }
        if (jobRequest.getResultConnectionSettings().isPresent()) {
            queryParam.put("result_connection_settings", jobRequest.getResultConnectionSettings().get());
        }

        if (jobRequest.getEngineVersion().isPresent()) {
            queryParam.put("engine_version", jobRequest.getEngineVersion().get().getEngineVersion());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("submit job: " + jobRequest);
        }

        TDJobSubmitResult result =
                doPost(
                        buildUrl("/v3/job/issue", jobRequest.getType().getType(), jobRequest.getDatabase()),
                        queryParam,
                        jobRequest.getConfig().map((config) -> {
                                ObjectNode body = config.objectNode();
                                body.set("config", config);
                                return body.toString();
                        }),
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
    public TDJobSummary jobStatus(String jobId)
            throws TDClientException
    {
        return doGet(buildUrl("/v3/job/status", jobId), TDJobSummary.class);
    }

    @Override
    public TDJobSummary jobStatusByDomainKey(String domainKey)
    {
        return doGet(buildUrl("/v3/job/status_by_domain_key", domainKey), TDJobSummary.class);
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
        return doGet(buildUrl("/v3/bulk_import/list"), new TypeReference<List<TDBulkImportSession>>() {});
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
        performBulkImportSession(sessionName, TDJob.Priority.NORMAL);
    }

    @Override
    public void performBulkImportSession(String sessionName, Optional<String> poolName)
    {
        performBulkImportSession(sessionName, poolName, TDJob.Priority.NORMAL);
    }

    @Override
    public void performBulkImportSession(String sessionName, TDJob.Priority priority)
    {
        performBulkImportSession(sessionName, Optional.empty(), priority);
    }

    @Override
    public void performBulkImportSession(String sessionName, Optional<String> poolName, TDJob.Priority priority)
    {
        Optional<String> jsonBody = Optional.empty();
        if (poolName.isPresent()) {
            jsonBody = Optional.of(toJSONString(Collections.singletonMap("pool_name", poolName.get())));
        }
        doPost(buildUrl("/v3/bulk_import/perform", sessionName), Collections.singletonMap("priority", Integer.toString(priority.toInt())), jsonBody, String.class);
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

    @Override
    public String startSavedQuery(String name, Date scheduledTime)
    {
        return startSavedQuery(TDSavedQueryStartRequest.builder()
                .name(name)
                .scheduledTime(scheduledTime)
                .build());
    }

    @Override
    public String startSavedQuery(long id, Date scheduledTime)
    {
        return startSavedQueryV4(TDSavedQueryStartRequest.builder()
                .name("")
                .id(id)
                .scheduledTime(scheduledTime)
                .build());
    }

    @Override
    public String startSavedQuery(TDSavedQueryStartRequest request)
    {
        if (request.id().isPresent()) {
            return startSavedQueryV4(request);
        }
        else {
            return startSavedQueryV3(request);
        }
    }

    private String startSavedQueryV4(TDSavedQueryStartRequest request)
    {
        if (request.num().isPresent() && request.num().get() != 1) {
            throw new UnsupportedOperationException("num must be 1");
        }

        TDSavedQueryStartResultV4 result =
                doPost(buildUrl("/v4/queries", Long.toString(request.id().get()), "jobs"),
                        Collections.emptyMap(),
                        Optional.of(toJson(TDSavedQueryStartRequestV4.from(request))),
                        TDSavedQueryStartResultV4.class);

        return result.getId();
    }

    private String startSavedQueryV3(TDSavedQueryStartRequest request)
    {
        Map<String, String> queryParams = new HashMap<>();

        Optional<Integer> num = request.num();
        if (num.isPresent()) {
            queryParams.put("num", Integer.toString(num.get()));
        }

        Optional<String> domainKey = request.domainKey();
        if (domainKey.isPresent()) {
            queryParams.put("domain_key", domainKey.get());
        }

        TDScheduleRunResult result =
                doPost(buildUrl("/v3/schedule/run", request.name(), Long.toString(request.scheduledTime().getTime() / 1000)),
                        queryParams,
                        TDScheduleRunResult.class);

        return result.getJobs().get(0).getJobId();
    }

    @Override
    public List<TDSavedQuery> listSavedQueries()
    {
        return doGet(buildUrl("/v3/schedule/list"), new TypeReference<List<TDSavedQuery>>() {});
    }

    @Override
    public TDSavedQueryHistory getSavedQueryHistory(String name)
    {
        return getSavedQueryHistory(name, 0L, 20L);
    }

    @Override
    public TDSavedQueryHistory getSavedQueryHistory(String name, Long from, Long to)
    {
        TDApiRequest.Builder builder = TDApiRequest.Builder
                .GET(buildUrl("/v3/schedule/history", name));
        if (from != null) {
            builder.addQueryParam("from", String.valueOf(from));
        }
        if (to != null) {
            builder.addQueryParam("to", String.valueOf(to));
        }
        TDApiRequest request = builder.build();
        return httpClient.call(request, apiKeyCache, TDSavedQueryHistory.class);
    }

    protected String toJson(Object any)
    {
        try {
            return httpClient.getObjectMapper().writeValueAsString(any);
        }
        catch (JsonProcessingException e) {
            logger.error("Failed to produce json", e);
            throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, String.format("Failed to create JSON string from %s", any));
        }
    }

    @Override
    public TDSavedQuery saveQuery(TDSaveQueryRequest request)
    {
        String json = toJson(request);
        logger.debug("saveQuery request:" + json);
        TDSavedQuery result =
                doPost(
                        buildUrl("/v3/schedule/create", request.getName()),
                        Collections.emptyMap(),
                        Optional.of(json),
                        TDSavedQuery.class);
        return result;
    }

    @Override
    public TDSavedQuery updateSavedQuery(String name, TDSavedQueryUpdateRequest request)
    {
        String json = request.toJson();
        logger.debug("updateSaveQuery request:" + json);
        TDSavedQuery result =
                doPost(
                        buildUrl("/v3/schedule/update", name),
                        Collections.emptyMap(),
                        Optional.of(json),
                        TDSavedQuery.class);
        return result;
    }

    @Override
    public TDSavedQuery deleteSavedQuery(String name)
    {
        TDSavedQuery result = doPost(
                buildUrl("/v3/schedule/delete", name),
                TDSavedQuery.class);
        return result;
    }

    @Override
    public String submitExportJob(TDExportJobRequest jobRequest)
            throws TDClientException
    {
        Map<String, String> queryParam = new HashMap<>();
        queryParam.put("from", Long.toString(jobRequest.getFrom().getTime() / 1000));
        queryParam.put("to", Long.toString(jobRequest.getTo().getTime() / 1000));
        queryParam.put("file_prefix", jobRequest.getFilePrefix());
        queryParam.put("file_format", jobRequest.getFileFormat().toString());
        queryParam.put("storage_type", "s3");
        queryParam.put("bucket", jobRequest.getBucketName());
        queryParam.put("access_key_id", jobRequest.getAccessKeyId());
        queryParam.put("secret_access_key", jobRequest.getSecretAccessKey());

        if (jobRequest.getPoolName().isPresent()) {
            queryParam.put("pool_name", jobRequest.getPoolName().get());
        }

        Optional<String> domainKey = jobRequest.getDomainKey();
        if (domainKey.isPresent()) {
            queryParam.put("domain_key", domainKey.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("submit job: " + jobRequest);
        }

        TDJobSubmitResult result =
                doPost(
                        buildUrl("/v3/export/run", jobRequest.getDatabase(), jobRequest.getTable()),
                        queryParam,
                        TDJobSubmitResult.class);
        return result.getJobId();
    }

    @Override
    public TDBulkLoadSessionStartResult startBulkLoadSession(String name)
    {
        TDBulkLoadSessionStartRequest request = TDBulkLoadSessionStartRequest.builder()
                .build();
        return startBulkLoadSession(name, request);
    }

    @Override
    public TDBulkLoadSessionStartResult startBulkLoadSession(String name, long scheduledTime)
    {
        TDBulkLoadSessionStartRequest request = TDBulkLoadSessionStartRequest.builder()
                .setScheduledTime(String.valueOf(scheduledTime))
                .build();
        return startBulkLoadSession(name, request);
    }

    @Override
    public TDBulkLoadSessionStartResult startBulkLoadSession(String name, TDBulkLoadSessionStartRequest request)
    {
        Map<String, String> queryParams = Collections.emptyMap();
        String payload = null;
        try {
            payload = ObjectMappers.compactMapper().writeValueAsString(request);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return doPost(buildUrl("/v3/bulk_loads", name, "jobs"),
                queryParams, Optional.of(payload),
                TDBulkLoadSessionStartResult.class);
    }

    @Override
    public long lookupConnection(String name)
    {
        return doGet(buildUrl("/v3/connections/lookup?name=" + UrlPathSegmentEscaper.escape(name)), TDConnectionLookupResult.class).getId();
    }

    @Override
    public String submitResultExportJob(TDExportResultJobRequest jobRequest)
    {
        Map<String, String> queryParam = new HashMap<>();
        if (!jobRequest.getResultConnectionId().isEmpty() && !jobRequest.getResultConnectionSettings().isEmpty()) {
            queryParam.put("result_connection_id", jobRequest.getResultConnectionId());
            queryParam.put("result_connection_settings", jobRequest.getResultConnectionSettings());
        }
        else if (!jobRequest.getResultOutput().isEmpty()) {
            queryParam.put("result", jobRequest.getResultOutput());
        }
        else {
            throw new IllegalStateException("Either resultOutput or a pair of resultConnectionId and resultConnectionSettings is required");
        }

        TDJobSubmitResult result = doPost(
                buildUrl("/v3/job/result_export", jobRequest.getJobId()),
                queryParam,
                TDJobSubmitResult.class);

        return result.getJobId();
    }

    @Override
    public Optional<TDTableDistribution> tableDistribution(String databaseName, String tableName)
    {
        try {
            TDTableDistribution distribution = doGet(buildUrl(String.format("/v3/table/distribution/%s/%s", databaseName, tableName)), TDTableDistribution.class);
            return Optional.of(distribution);
        }
        catch (TDClientHttpNotFoundException e) {
            return Optional.empty();
        }
    }

    @Override
    public TDImportResult importFile(String databaseName, String tableName, File file)
    {
        return doPut(buildUrl(String.format("/v3/table/import/%s/%s/%s", databaseName, tableName, "msgpack.gz")), Collections.emptyMap(), file, TDImportResult.class);
    }

    @Override
    public TDImportResult importFile(String databaseName, String tableName, File file, String id)
    {
        return doPut(buildUrl(String.format("/v3/table/import_with_id/%s/%s/%s/%s", databaseName, tableName, id, "msgpack.gz")), Collections.emptyMap(), file, TDImportResult.class);
    }

    @Override
    public TDImportResult importBytes(String databaseName, String tableName, byte[] content)
    {
        return doPut(buildUrl(String.format("/v3/table/import/%s/%s/%s", databaseName, tableName, "msgpack.gz")), Collections.emptyMap(), content, 0, content.length, TDImportResult.class);
    }

    @Override
    public TDImportResult importBytes(String databaseName, String tableName, byte[] content, int offset, int length)
    {
        return doPut(buildUrl(String.format("/v3/table/import/%s/%s/%s", databaseName, tableName, "msgpack.gz")), Collections.emptyMap(), content, offset, length, TDImportResult.class);
    }

    @Override
    public TDImportResult importBytes(String databaseName, String tableName, byte[] content, String id)
    {
        return doPut(buildUrl(String.format("/v3/table/import_with_id/%s/%s/%s/%s", databaseName, tableName, id, "msgpack.gz")), Collections.emptyMap(), content, 0, content.length, TDImportResult.class);
    }

    @Override
    public TDImportResult importBytes(String databaseName, String tableName, byte[] content, int offset, int length, String id)
    {
        return doPut(buildUrl(String.format("/v3/table/import_with_id/%s/%s/%s/%s", databaseName, tableName, id, "msgpack.gz")), Collections.emptyMap(), content, offset, length, TDImportResult.class);
    }

    @Override
    public List<TDFederatedQueryConfig> getFederatedQueryConfigs()
        throws TDClientException
    {
        return doGet("/v4/federated_query_configs", new TypeReference<List<TDFederatedQueryConfig>>() {});
    }
}
