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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.json.simple.JSONValue;
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
import com.treasure_data.model.DeletePartialTableRequest;
import com.treasure_data.model.DeletePartialTableResult;
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

public class DefaultClientAdaptorImpl extends AbstractClientAdaptor
        implements DefaultClientAdaptor {

    private static Logger LOG = Logger.getLogger(DefaultClientAdaptorImpl.class.getName());

    private Validator validator;

    private HttpConnectionImpl conn = null;

    DefaultClientAdaptorImpl(Config conf) {
	super(conf);
	validator = new Validator();
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, map);
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
                validator.validateJSONData(jsonData);

                @SuppressWarnings("rawtypes")
                Map map = (Map) JSONValue.parse(jsonData);
                validator.validateJavaObject(jsonData, map);

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
        validator.validateCredentials(this, request);

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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, map);

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
        validator.validateDatabaseName(request.getDatabaseName());
        request.setCredentials(getConfig().getCredentials());
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_DATABASE_CREATE,
                    HttpConnectionImpl.e(request.getDatabaseName()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, dbMap);

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
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_DATABASE_DELETE,
                    HttpConnectionImpl.e(request.getDatabaseName()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, dbMap);

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
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_LIST,
                    HttpConnectionImpl.e(request.getDatabase().getName()));
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
            validator.validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

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
        validator.validateTableName(request.getTableName());
        request.setCredentials(getConfig().getCredentials());
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_CREATE,
                    HttpConnectionImpl.e(request.getDatabase().getName()),
                    HttpConnectionImpl.e(request.getTableName()),
                    HttpConnectionImpl.e(Table.toTypeName(request.getTable().getType())));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, tableMap);

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
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_DELETE,
                    HttpConnectionImpl.e(request.getDatabase().getName()),
                    HttpConnectionImpl.e(request.getTable().getName()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, tableMap);

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
    public DeletePartialTableResult deletePartialTable(DeletePartialTableRequest request)
            throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_DELETE_PARTIAL,
                    HttpConnectionImpl.e(request.getDatabase().getName()),
                    HttpConnectionImpl.e(request.getTable().getName()));
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            params.put("from", "" + request.getFrom());
            params.put("to", "" + request.getTo());
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Delete partial table failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            jsonData = conn.getResponseBody();
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, jobMap);

        String jobID = jobMap.get("job_id");
        String dbName = jobMap.get("database");
        if (!dbName.equals(request.getDatabase().getName())) {
            String msg = String.format("invalid database name: expected=%s, actual=%s",
                    request.getDatabase().getName(), dbName);
            throw new ClientException(msg);
        }

        Job job = new Job(jobID, Job.Type.MAPRED, request.getDatabase(), null, null);
        return new DeletePartialTableResult(job);
    }

    @Override
    public ImportResult importData(ImportRequest request) throws ClientException {
        request.setCredentials(getConfig().getCredentials());
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_TABLE_IMPORT,
                    HttpConnectionImpl.e(request.getTable().getDatabase().getName()),
                    HttpConnectionImpl.e(request.getTable().getName()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, map);

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
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_EXPORTJOB_SUBMIT,
                    HttpConnectionImpl.e(request.getDatabase().getName()), HttpConnectionImpl.e(request.getTable().getName()));
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            if (request.getAccessKeyID() != null) {
                params.put("access_key_id", HttpConnectionImpl.e(request.getAccessKeyID()));
            } else {
                throw new IllegalArgumentException("access_key_id is null");
            }
            if (request.getSecretAccessKey() != null) {
                params.put("secret_access_key", HttpConnectionImpl.e(request.getSecretAccessKey()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, jobMap);

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
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_SUBMIT,
                    HttpConnectionImpl.e(request.getDatabase().getName()));
            Map<String, String> header = null;
            Map<String, String> params = new HashMap<String, String>();
            if (request.getJob().getQuery() != null) {
                // query is required
                params.put("query", HttpConnectionImpl.e(request.getJob().getQuery()));
            } else {
                throw new IllegalArgumentException("query is null");
            }
            params.put("version", "0.7");
            if (request.getJob().getResultTable() != null) {
                // result table is not required
                params.put("result", HttpConnectionImpl.e(request.getJob().getResultTable()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, jobMap);

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
        validator.validateCredentials(this, request);

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
            validator.validateJSONData(jsonData);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

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
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_KILL,
                    HttpConnectionImpl.e(request.getJob().getJobID()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, map);

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
        validator.validateCredentials(this, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_SHOW,
                    HttpConnectionImpl.e(request.getJob().getJobID()));
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
            validator.validateJSONData(jsonData);
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
        validator.validateJavaObject(jsonData, jobMap);

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
        validator.validateCredentials(this, request);

        Unpacker unpacker = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_JOB_RESULT,
                    HttpConnectionImpl.e(request.getJobResult().getJob().getJobID()));
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

    static interface HttpURL {
        String V3_USER_AUTHENTICATE = "/v3/user/authenticate";

        String V3_SYSTEM_SERVER_STATUS = "/v3/system/server_status";

        String V3_DATABASE_LIST = "/v3/database/list";

        String V3_DATABASE_CREATE = "/v3/database/create/%s";

        String V3_DATABASE_DELETE = "/v3/database/delete/%s";

        String V3_TABLE_LIST = "/v3/table/list/%s";

        String V3_TABLE_CREATE = "/v3/table/create/%s/%s/%s";

        String V3_TABLE_DELETE = "/v3/table/delete/%s/%s";

        String V3_TABLE_DELETE_PARTIAL = "/v3/table/partialdelete/%s/%s";

        String V3_TABLE_IMPORT = "/v3/table/import/%s/%s/msgpack.gz";

        String V3_EXPORTJOB_SUBMIT = "/v3/export/run/%s/%s";

        String V3_JOB_SUBMIT = "/v3/job/issue/hive/%s";

        String V3_JOB_LIST = "/v3/job/list";

        String V3_JOB_KILL = "/v3/job/kill/%s";

        String V3_JOB_SHOW = "/v3/job/show/%s";

        String V3_JOB_RESULT = "/v3/job/result/%s";
    }
}
