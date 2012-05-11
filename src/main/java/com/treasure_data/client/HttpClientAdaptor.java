//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.json.simple.JSONValue;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.AuthenticateResult;
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.DeleteDatabaseResult;
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.DeleteTableResult;
import com.treasure_data.model.ExportRequest;
import com.treasure_data.model.ExportResult;
import com.treasure_data.model.ImportRequest;
import com.treasure_data.model.ImportResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.KillJobRequest;
import com.treasure_data.model.KillJobResult;
import com.treasure_data.model.ListDatabases;
import com.treasure_data.model.ListDatabasesRequest;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.ListJobs;
import com.treasure_data.model.ListJobsRequest;
import com.treasure_data.model.ListJobsResult;
import com.treasure_data.model.ListTables;
import com.treasure_data.model.ListTablesRequest;
import com.treasure_data.model.ListTablesResult;
import com.treasure_data.model.Request;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.ServerStatus;
import com.treasure_data.model.GetServerStatusRequest;
import com.treasure_data.model.GetServerStatusResult;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.Table;
import com.treasure_data.model.TableSummary;

public class HttpClientAdaptor extends AbstractClientAdaptor {

    private static Logger LOG = Logger.getLogger(HttpClientAdaptor.class.getName());

    public static String e(String s) throws ClientException {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ClientException(e);
        }
    }

    private HttpConnectionImpl conn = null;

    HttpClientAdaptor(Config conf) {
	super(conf);
    }

    HttpConnectionImpl getConnection() {
        return conn;
    }

    HttpConnectionImpl createConnection() {
        if (conn == null) {
            conn = new HttpConnectionImpl();
        }
        return conn;
    }

    void setConnection(HttpConnectionImpl conn) {
        this.conn = conn;
    }

    @Override
    public AuthenticateResult authenticate(AuthenticateRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = HttpURL.V3_USER_AUTHENTICATE;
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            params.put("user", request.getEmail());
            params.put("password", request.getPassword());
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Authentication failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, map);
        //String user = map.get("user");
        String apiKey = map.get("apikey");
        TreasureDataCredentials credentails = new TreasureDataCredentials(apiKey);
        return new AuthenticateResult(credentails);
    }

    @Override
    public GetServerStatusResult getServerStatus(GetServerStatusRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());

        try {
            conn = createConnection();

            // send request
            String path = HttpURL.V3_SYSTEM_SERVER_STATUS;
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doGetRequest(request, path, header, params);

            // receive response code and body
            String msg = null;
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                msg = String.format("Server is down (%s (%d): %s)",
                        conn.getResponseMessage(), code, conn.getResponseBody());
                LOG.severe(msg);
            } else {
                String jsonData = conn.getResponseBody();
                validateJSONData(jsonData);

                @SuppressWarnings("rawtypes")
                Map map = (Map) JSONValue.parse(jsonData);
                validateJavaObject(jsonData, map);

                msg = (String) map.get("status");
            }
            return new GetServerStatusResult(new ServerStatus(msg));
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    @Override
    public ListDatabasesResult listDatabases(ListDatabasesRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = HttpURL.V3_DATABASE_LIST;
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doGetRequest(request, path, header, params);

            // receive response code and body
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List databases failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, map);

        @SuppressWarnings("unchecked")
        Iterator<Map<String, Object>> dbMaps =
            ((List<Map<String, Object>>) map.get("databases")).iterator();
        List<DatabaseSummary> databases = new ArrayList<DatabaseSummary>();
        while (dbMaps.hasNext()) {
            Map<String, Object> dbMap = dbMaps.next();
            String name = (String) dbMap.get("name");
            long count = (Long) dbMap.get("count");
            String createdAt = (String) dbMap.get("created_at");
            String updatedAt = (String) dbMap.get("updated_at");
            databases.add(new DatabaseSummary(name, count, createdAt, updatedAt));
        }

        return new ListDatabasesResult(new ListDatabases<DatabaseSummary>(databases));
    }

    @Override
    public CreateDatabaseResult createDatabase(CreateDatabaseRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_DATABASE_CREATE,
                    e(request.getDatabaseName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Create database failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> dbMap = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, dbMap);

        String dbName = dbMap.get("database");
        if (!dbName.equals(request.getDatabaseName())) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getDatabaseName(), dbName);
            throw new ClientException(msg);
        }

        return new CreateDatabaseResult(new Database(dbName));
    }

    @Override
    public DeleteDatabaseResult deleteDatabase(DeleteDatabaseRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_DATABASE_DELETE,
                    e(request.getDatabaseName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Delete database failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> dbMap = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, dbMap);

        String dbName = dbMap.get("database");
        if (!dbName.equals(request.getDatabaseName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getDatabaseName(), dbName);
            throw new ClientException(msg);
        }

        return new DeleteDatabaseResult(request.getDatabase());
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest request)
            throws ClientException {
        // validate request
        if (request.getDatabase() == null) {
            throw new ClientException("database is not specified");
        }

        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_LIST,
                    e(request.getDatabase().getName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doGetRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List tables failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, map);

        String dbName = (String) map.get("database");
        if (!dbName.equals(request.getDatabase().getName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getDatabase().getName(), dbName);
            throw new ClientException(msg);
        }

        @SuppressWarnings("unchecked")
        Iterator<Map<String, Object>> tableMapIter =
            ((List<Map<String, Object>>) map.get("tables")).iterator();
        List<TableSummary> tableList = new ArrayList<TableSummary>();
        while (tableMapIter.hasNext()) {
            Map<String, Object> tableMap = tableMapIter.next();
            String name = (String) tableMap.get("name");
            String typeName = (String) tableMap.get("type");
            Long count = (Long) tableMap.get("count");
            String schema = (String) tableMap.get("schema");
            String createdAt = (String) tableMap.get("created_at");
            String updatedAt = (String) tableMap.get("updated_at");
            TableSummary tbl = new TableSummary(request.getDatabase(), name, Table.toType(typeName),
                    count, schema, createdAt, updatedAt);
            tableList.add(tbl);
        }

        ListTables<TableSummary> tables = new ListTables<TableSummary>(tableList);
        return new ListTablesResult(request.getDatabase(), tables);
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_CREATE,
                    e(request.getDatabase().getName()),
                    e(request.getTableName()),
                    e(Table.toTypeName(request.getTable().getType())));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Create table failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> tableMap = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, tableMap);

        String dbName = tableMap.get("database");
        if (!dbName.equals(request.getDatabase().getName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getDatabase().getName(), dbName);
            throw new ClientException(msg);
        }
        String tableName = tableMap.get("table");
        if (!tableName.equals(request.getTableName())) {
            String msg = String.format("invalid table name: expected=%s, actual=%s",
                    request.getTableName(), dbName);
            throw new ClientException(msg);
        }
        Table.Type tableType = Table.toType(tableMap.get("type"));
        if (tableType != request.getTable().getType()) {
            String msg = String.format("invalid table type: expected=%s, actual=%s",
                    request.getTable().getType(), tableType);
            throw new ClientException(msg);
        }

        Table table = new Table(request.getDatabase(), tableName, tableType);
        return new CreateTableResult(table);
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_DELETE,
                    e(request.getDatabase().getName()),
                    e(request.getTable().getName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Delete table failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> tableMap = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, tableMap);

        String dbName = tableMap.get("database");
        if (!dbName.equals(request.getDatabase().getName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getDatabase().getName(), dbName);
            throw new ClientException(msg);
        }
        String tableName = tableMap.get("table");
        if (!tableName.equals(request.getTable().getName())) {
            String msg = String.format("invalid table name: expected=%s, actual=%s",
                    request.getTable().getName(), dbName);
            throw new ClientException(msg);
        }

        return new DeleteTableResult(request.getDatabase(), tableName);
    }

    @Override
    public ImportResult importData(ImportRequest request) throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_IMPORT,
                    e(request.getTable().getDatabase().getName()),
                    e(request.getTable().getName()));
            conn.doPutRequest(request, path, request.getBytes());

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Import data failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, map);

        String dbName = (String) map.get("database");
        String tblName = (String) map.get("table");
        double elapsedTime = (Double) map.get("elapsed_time");
        if (!dbName.equals(request.getTable().getDatabase().getName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getTable().getDatabase().getName(), dbName);
            throw new ClientException(msg);
        }
        if (!tblName.equals(request.getTable().getName())) {
            String msg = String.format("invalid table name: expected=%s, actual=%s",
                    request.getTable().getDatabase().getName(), tblName);
            throw new ClientException(msg);
        }

        return new ImportResult(request.getTable(), elapsedTime);
    }

    @Override
    public ExportResult exportData(ExportRequest request) throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_EXPORTJOB_SUBMIT,
                    e(request.getDatabase().getName()), e(request.getTable().getName()));
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            if (request.getAccessKeyID() != null) {
                params.put("access_key_id", e(request.getAccessKeyID()));
            } else {
                throw new IllegalArgumentException("access_key_id is null");
            }
            if (request.getSecretAccessKey() != null) {
                params.put("secret_access_key", e(request.getSecretAccessKey()));
            } else {
                throw new IllegalArgumentException("secret_access_key is null");
            }
            if (request.getStorageType() != null) {
                params.put("storage_type", request.getStorageType());
            } else {
                throw new IllegalArgumentException("storage_type is null");
            }
            if (request.getBucketName() != null) {
                params.put("bucket", request.getBucketName());
            } else {
                throw new IllegalArgumentException("bucket is null");
            }
            if (request.getFileFormat() != null) {
                params.put("file_format", request.getFileFormat());
            } else {
                throw new IllegalArgumentException("file_format is null");
            }
            if (request.getFrom() != null) {
                params.put("from", request.getFrom());
            }
            if (request.getTo() != null) {
                params.put("to", request.getTo());
            }
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Submit job failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> jobMap = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, jobMap);

        String jobID = jobMap.get("job_id");
        String dbName = jobMap.get("database");
        if (!dbName.equals(request.getDatabase().getName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getDatabase().getName(), dbName);
            throw new ClientException(msg);
        }

        Job job = new Job(jobID, Job.Type.MAPRED, request.getDatabase(), null, null);
        return new ExportResult(job);
    }

    @Override
    public SubmitJobResult submitJob(SubmitJobRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_SUBMIT,
                    e(request.getDatabase().getName()));
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            if (request.getJob().getQuery() != null) {
                // query is required
                params.put("query", e(request.getJob().getQuery()));
            } else {
                throw new IllegalArgumentException("query is null");
            }
            params.put("version", "0.7");
            if (request.getJob().getResultTable() != null) {
                // result table is not required
                params.put("result", e(request.getJob().getResultTable()));
            }
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Submit job failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> jobMap = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, jobMap);

        String jobID = jobMap.get("job_id");
        String dbName = jobMap.get("database");
        if (!dbName.equals(request.getDatabase().getName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getDatabase().getName(), dbName);
            throw new ClientException(msg);
        }

        Job job = request.getJob();
        job.setJobID(jobID);
        return new SubmitJobResult(job);
    }

    @Override
    public ListJobsResult listJobs(ListJobsRequest request) throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_LIST);
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            long from = request.getFrom();
            if (from < 0) {
                throw new IllegalArgumentException();
            } else {
                params.put("from", "" + from);
            }
            long to = request.getTo();
            if (to < 0) {
                throw new IllegalArgumentException();
            } else if (to > 0) {
                params.put("to", "" + to);
            }
            conn.doGetRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List jobs failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, map);

        long count = (Long) map.get("count");
        long from = (Long) map.get("from");
        long to = (Long) map.get("to");
        @SuppressWarnings("unchecked")
        Iterator<Map<String, String>> jobMapIter =
            ((List<Map<String, String>>) map.get("jobs")).iterator();
        List<JobSummary> jobs = new ArrayList<JobSummary>();
        while (jobMapIter.hasNext()) {
            Map<String, String> jobMap = jobMapIter.next();
            Job.Type type = Job.toType(jobMap.get("type"));
            String jobID = jobMap.get("job_id");
            JobSummary.Status status = JobSummary.toStatus(jobMap.get("status"));
            String startAt = jobMap.get("start_at");
            String endAt = jobMap.get("end_at");
            String query = jobMap.get("query");
            String result = jobMap.get("result");
            JobSummary job = new JobSummary(jobID, type, null, null, result,
                    status, startAt, endAt, query, null);
            jobs.add(job);
        }

        return new ListJobsResult(new ListJobs<JobSummary>(count, from, to, jobs));
    }

    @Override
    public KillJobResult killJob(KillJobRequest request) throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_KILL,
                    e(request.getJob().getJobID()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Kill job failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // parse JSON data
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, map);

        JobSummary.Status status = JobSummary.toStatus(map.get("former_status"));
        String jobID = map.get("job_id");
        if (!jobID.equals(request.getJob().getJobID())) {
            String msg = String.format("invalid job ID: expected=%s, actual=%s",
                    request.getJob().getJobID(), jobID);
            throw new ClientException(msg);
        }

        return new KillJobResult(jobID, status);
    }

    @Override
    public ShowJobResult showJob(ShowJobRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_SHOW,
                    e(request.getJob().getJobID()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doGetRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Show jobs failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, String> jobMap =
            (Map<String, String>) JSONValue.parse(jsonData);
        validateJavaObject(jsonData, jobMap);

        Job.Type type = Job.toType(jobMap.get("type"));
        String jobID = jobMap.get("job_id");
        JobSummary.Status status = JobSummary.toStatus(jobMap.get("status"));
        String query = jobMap.get("query");
        String result = jobMap.get("result");
        String resultSchema = jobMap.get("hive_result_schema");
        // TODO different object from request's one
        JobSummary job = new JobSummary(jobID, type, null, null, result,
                status, null, null, query, resultSchema);
        return new ShowJobResult(job);
    }

    @Override
    public GetJobResultResult getJobResult(GetJobResultRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validateCredentials(request);

        Unpacker unpacker = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_RESULT,
                    e(request.getJobResult().getJob().getJobID()));
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            if (request.getJobResult().getFormat() != JobResult.Format.MSGPACK) {
                String msg = String.format("Doesn't support format",
                        request.getJobResult().getFormat());
                throw new UnsupportedOperationException(msg);
            } else {
                params.put("format", JobResult.toFormatName(JobResult.Format.MSGPACK));
            }
            conn.doGetRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Get job result failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            unpacker = conn.getResponseBodyBinary();
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        request.getJobResult().setResult(unpacker);
        return new GetJobResultResult(request.getJobResult());
    }

    private void validateCredentials(Request<?> request) throws ClientException {
        String apiKey = request.getCredentials().getAPIKey();
        if (apiKey == null) {
            throw new ClientException("api key is not set.");
        }
    }

    private void validateJSONData(String jsonData) throws ClientException {
        if (jsonData == null) {
            throw new ClientException(
                    "JSON data that was returned by server is null");
        }
    }

    private void validateJavaObject(String jsonData, Object obj) throws ClientException {
        if (obj == null) {
            throw new ClientException(String.format(
                    "Server error (invalid JSON Data): %s", jsonData));
        }
    }

    static interface HttpURL {
        String V3_USER_AUTHENTICATE = "/v3/user/authenticate";

        String V3_SYSTEM_SERVER_STATUS = "/v3/system/server_status";

        String V3_DATABASE_LIST = "/v3/database/list";

        String V3_DATABASE_CREATE = "/v3/database/create/%s";

        String V3_DATABASE_DELETE = "/v3/database/delete/%s";

        String V3_TABLE_LIST = "/v3/table/list/%s";

        String V3_TABLE_CREATE = "/v3/table/create/%s/%s/%s";

        String V3_TABLE_DELETE = "/v3/table/delete/%s/%s";

        String V3_TABLE_IMPORT = "/v3/table/import/%s/%s/msgpack.gz";

        String V3_EXPORTJOB_SUBMIT = "/v3/export/run/%s/%s";

        String V3_JOB_SUBMIT = "/v3/job/issue/hive/%s";

        String V3_JOB_LIST = "/v3/job/list";

        String V3_JOB_KILL = "/v3/job/kill/%s";

        String V3_JOB_SHOW = "/v3/job/show/%s";

        String V3_JOB_RESULT = "/v3/job/result/%s";
    }

    static class HttpConnectionImpl {
        private static final SimpleDateFormat RFC2822FORMAT =
            new SimpleDateFormat( "E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH );

        private HttpURLConnection conn = null;

        HttpConnectionImpl() {
        }

        void doGetRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append("http://").append(getApiServerPath()).append(path);

            // parameters
            if (params != null && !params.isEmpty()) {
                sbuf.append("?");
                int paramSize = params.size();
                Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
                for (int i = 0; i < paramSize; ++i) {
                    Map.Entry<String, String> e = iter.next();
                    sbuf.append(e.getKey()).append("=").append(e.getValue());
                    if (i + 1 != paramSize) {
                        sbuf.append("&");
                    }
                }
            }

            // create connection object with url
            URL url = new URL(sbuf.toString());
            conn = (HttpURLConnection) url.openConnection();

            // header
            conn.setRequestMethod("GET");
            String apiKey = request.getCredentials().getAPIKey();
            if (apiKey != null) {
                conn.setRequestProperty("Authorization", "TD1 " + apiKey);
            }
            conn.setRequestProperty("Date", toRFC2822Format(new Date()));
            if (header != null && !header.isEmpty()) {
                for (Map.Entry<String, String> e : header.entrySet()) {
                    conn.setRequestProperty(e.getKey(), e.getValue());
                }
            }

            // do connection to server
            conn.connect();
        }

        void doPostRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append("http://").append(getApiServerPath()).append(path);

            // parameters
            if (params != null && !params.isEmpty()) {
                sbuf.append("?");
                int paramSize = params.size();
                Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
                for (int i = 0; i < paramSize; ++i) {
                    Map.Entry<String, String> e = iter.next();
                    sbuf.append(e.getKey()).append("=").append(e.getValue());
                    if (i + 1 != paramSize) {
                        sbuf.append("&");
                    }
                }
                URL url = new URL(sbuf.toString());
                conn = (HttpURLConnection) url.openConnection();
            } else {
                URL url = new URL(sbuf.toString());
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestProperty("Content-Length", "0");
            }

            // header
            conn.setRequestMethod("POST");
            String apiKey = request.getCredentials().getAPIKey();
            if (apiKey != null) {
                conn.setRequestProperty("Authorization", "TD1 " + apiKey);
            }
            conn.setRequestProperty("Date", toRFC2822Format(new Date()));
            if (header != null && !header.isEmpty()) {
                for (Map.Entry<String, String> e : header.entrySet()) {
                    conn.setRequestProperty(e.getKey(), e.getValue());
                }
            }
            conn.connect();
        }

        HttpURLConnection doPutRequest(Request<?> request, String path, byte[] bytes)
                throws IOException {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append("http://").append(getApiServerPath()).append(path);

            URL url = new URL(sbuf.toString());
            conn = (HttpURLConnection) url.openConnection();
            conn.setReadTimeout(600 * 1000);
            conn.setRequestMethod("PUT");
            //conn.setRequestProperty("Content-Type", "application/octet-stream");
            conn.setRequestProperty("Content-Length", "" + bytes.length);

            String apiKey = request.getCredentials().getAPIKey();
            if (apiKey != null) {
                conn.setRequestProperty("Authorization", "TD1 " + apiKey);
            }
            conn.setRequestProperty("Date", toRFC2822Format(new Date()));
            conn.setDoOutput(true);
            conn.setUseCaches (false);
            //conn.connect();

            // body
            BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
            out.write(bytes);
            out.flush();
            //out.close();

            return conn;
        }

        int getResponseCode() throws IOException {
            return conn.getResponseCode();
        }

        String getResponseMessage() throws IOException {
            return conn.getResponseMessage();
        }

        String getResponseBody() throws IOException {
            StringBuilder sbuf = new StringBuilder();
            BufferedReader reader = new BufferedReader( 
                    new InputStreamReader(conn.getInputStream()));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                sbuf.append(line);
            }
            reader.close();
            return sbuf.toString();
        }

        void disconnect() {
            conn.disconnect();
        }

        Unpacker getResponseBodyBinary() throws IOException {
            BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
            MessagePack msgpack = new MessagePack();
            BufferUnpacker unpacker = msgpack.createBufferUnpacker();
            byte[] buf = new byte[1024];

            int len = 0;
            while ((len = in.read(buf)) != -1) {
                unpacker.feed(buf, 0, len);
            }

            return unpacker;
        }

        private String getApiServerPath() {
            String hostAndPort = "";

            // environment variables
            hostAndPort = System.getenv(Config.TD_ENV_API_SERVER);
            if (hostAndPort != null) {
                return hostAndPort;
            }

            // system properties
            Properties props = System.getProperties();
            String host = props.getProperty(
                    Config.TD_API_SERVER_HOST, Config.TD_API_SERVER_HOST_DEFAULT);
            int port = Integer.parseInt(props.getProperty(
                    Config.TD_API_SERVER_PORT, Config.TD_API_SERVER_PORT_DEFAULT));
            hostAndPort = host + ":" + port;

            return hostAndPort;
        }

        private static String toRFC2822Format(Date from) {
            return RFC2822FORMAT.format(from);
        }
    }
}
