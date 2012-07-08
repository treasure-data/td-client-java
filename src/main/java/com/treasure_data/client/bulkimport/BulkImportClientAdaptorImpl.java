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
package com.treasure_data.client.bulkimport;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.json.simple.JSONValue;

import com.treasure_data.client.ClientAdaptor;
import com.treasure_data.client.ClientException;
import com.treasure_data.client.HttpClientAdaptor;
import com.treasure_data.client.HttpConnectionImpl;
import com.treasure_data.client.Validator;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.DeleteTableResult;
import com.treasure_data.model.ImportResult;
import com.treasure_data.model.ListDatabases;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.bulkimport.CommitSessionRequest;
import com.treasure_data.model.bulkimport.CommitSessionResult;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeleteFileRequest;
import com.treasure_data.model.bulkimport.DeleteFileResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.FreezeSessionRequest;
import com.treasure_data.model.bulkimport.FreezeSessionResult;
import com.treasure_data.model.bulkimport.GetErrorRecordsRequest;
import com.treasure_data.model.bulkimport.GetErrorRecordsResult;
import com.treasure_data.model.bulkimport.ListFilesRequest;
import com.treasure_data.model.bulkimport.ListFilesResult;
import com.treasure_data.model.bulkimport.ListSessions;
import com.treasure_data.model.bulkimport.ListSessionsRequest;
import com.treasure_data.model.bulkimport.ListSessionsResult;
import com.treasure_data.model.bulkimport.PerformSessionRequest;
import com.treasure_data.model.bulkimport.PerformSessionResult;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;
import com.treasure_data.model.bulkimport.UnfreezeSessionRequest;
import com.treasure_data.model.bulkimport.UnfreezeSessionResult;
import com.treasure_data.model.bulkimport.UploadFileRequest;
import com.treasure_data.model.bulkimport.UploadFileResult;

public class BulkImportClientAdaptorImpl implements BulkImportClientAdaptor {
    private static Logger LOG = Logger.getLogger(BulkImportClientAdaptorImpl.class.getName());

    static interface HttpURL {
        /*
  get  'v3/bulk_import/list'                => 'bulk_import#list'
  post 'v3/bulk_import/create/:name/:database/:table' => 'bulk_import#create'
  post 'v3/bulk_import/delete/:name'        => 'bulk_import#delete'
  put  'v3/bulk_import/upload_part/:name/:part'  => 'bulk_import#upload_part'
  post 'v3/bulk_import/delete_part/:name/:part'  => 'bulk_import#delete_part'
  get  'v3/bulk_import/list_parts/:name'    => 'bulk_import#list_parts'
  post 'v3/bulk_import/freeze/:name'        => 'bulk_import#freeze'
  post 'v3/bulk_import/unfreeze/:name'      => 'bulk_import#unfreeze'
  post 'v3/bulk_import/perform/:name'       => 'bulk_import#perform'
  post 'v3/bulk_import/commit/:name'        => 'bulk_import#commit'
  get  'v3/bulk_import/error_records/:name' => 'bulk_import#error_records'
  post 'v3/bulk_import/perform_finished/:id' => 'bulk_import#perform_finished'
  post 'v3/bulk_import/commit_finished/:id'  => 'bulk_import#commit_finished'
         */

        String V3_LIST = "/v3/bulk_import/list";

        String V3_CREATE = "/v3/bulk_import/create/%s/%s/%s";

        String V3_DELETE = "/v3/bulk_import/delete/%s";

        String V3_UPLOAD_PART = "/v3/bulk_import/upload_part/%s/%s";

        String V3_DELETE_PART = "/v3/bulk_import/delete_part/&s/%s";

        String V3_LIST_PARTS = "/v3/bulk_import/list_parts/%s";

        String V3_FREEZE = "/v3/bulk_import/freeze/%s";

        String V3_UNFREEZE = "/v3/bulk_import/unfreeze/%s";

        String V3_PERFORM = "/v3/bulk_import/perform/%s";

        String V3_COMMIT = "/v3/bulk_import/commit/%s";

        String V3_ERROR_RECORDS = "/v3/bulk_import/error_records/%s";

        String V3_PERFORM_FINISHED = "/v3/bulk_import/perform_finished/%s";

        String V3_COMMIT_FINISHED = "/v3/bulk_import/commit_finished/%s";
    }

    public static String e(String s) throws ClientException {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ClientException(e);
        }
    }

    private ClientAdaptor clientAdaptor;

    private Validator validator;

    private HttpConnectionImpl conn;

    BulkImportClientAdaptorImpl(ClientAdaptor clientAdaptor) {
        this.clientAdaptor = clientAdaptor;
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
    public ListSessionsResult listSessions(ListSessionsRequest request)
            throws ClientException {
        return null;
//        request.setCredentials(clientAdaptor.getConfig().getCredentials());
//        validator.checkCredentials(clientAdaptor, request);
//
//        String jsonData = null;
//        try {
//            conn = createConnection();
//
//            // send request
//            String path = HttpURL.V3_LIST;
//            Map<String, String> header = null;
//            Map<String, String> params = null;
//            conn.doGetRequest(request, path, header, params);
//
//            // receive response code and body
//            int code = conn.getResponseCode();
//            if (code != HttpURLConnection.HTTP_OK) {
//                String msg = String.format("List databases failed (%s (%d): %s)",
//                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
//                LOG.severe(msg);
//                throw new ClientException(msg);
//            }
//
//            // receive response body
//            jsonData = conn.getResponseBody();
//            validator.validateJSONData(jsonData);
//        } catch (IOException e) {
//            throw new ClientException(e);
//        } finally {
//            if (conn != null) {
//                conn.disconnect();
//            }
//        }
//
//        // parse JSON data
//        @SuppressWarnings("rawtypes")
//        Map map = (Map) JSONValue.parse(jsonData);
//        validator.validateJavaObject(jsonData, map);
//
//        // TODO #MN "bulk_imports"
//        @SuppressWarnings("unchecked")
//        Iterator<Map<String, Object>> dbMaps =
//            ((List<Map<String, Object>>) map.get("databases")).iterator();
//        List<DatabaseSummary> databases = new ArrayList<DatabaseSummary>();
//        while (dbMaps.hasNext()) {
//            Map<String, Object> dbMap = dbMaps.next();
//            String name = (String) dbMap.get("name");
//            long count = (Long) dbMap.get("count");
//            String createdAt = (String) dbMap.get("created_at");
//            String updatedAt = (String) dbMap.get("updated_at");
//            databases.add(new DatabaseSummary(name, count, createdAt, updatedAt));
//        }
//
//        return new ListSessionsResult(new ListSessions<SessionSummary>(sessions));
    }

    @Override
    public ListFilesResult listFiles(ListFilesRequest request)
            throws ClientException {
        return null;
//        request.setCredentials(clientAdaptor.getConfig().getCredentials());
//        validator.checkCredentials(clientAdaptor, request);
//
//        String jsonData = null;
//        try {
//            conn = createConnection();
//
//            // send request
//            String path = String.format(HttpURL.V3_LIST_PARTS,
//                    e(request.getSessionName()));
//            Map<String, String> header = null;
//            Map<String, String> params = null;
//            conn.doGetRequest(request, path, header, params);
//
//            // receive response code and body
//            int code = conn.getResponseCode();
//            if (code != HttpURLConnection.HTTP_OK) {
//                String msg = String.format("List databases failed (%s (%d): %s)",
//                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
//                LOG.severe(msg);
//                throw new ClientException(msg);
//            }
//
//            // receive response body
//            jsonData = conn.getResponseBody();
//            validator.validateJSONData(jsonData);
//        } catch (IOException e) {
//            throw new ClientException(e);
//        } finally {
//            if (conn != null) {
//                conn.disconnect();
//            }
//        }
//
//        // parse JSON data
//        @SuppressWarnings("rawtypes")
//        Map map = (Map) JSONValue.parse(jsonData);
//        validator.validateJavaObject(jsonData, map);
//
//        // TODO #MN "parts"
//        @SuppressWarnings("unchecked")
//        Iterator<Map<String, Object>> dbMaps =
//            ((List<Map<String, Object>>) map.get("databases")).iterator();
//        List<DatabaseSummary> databases = new ArrayList<DatabaseSummary>();
//        while (dbMaps.hasNext()) {
//            Map<String, Object> dbMap = dbMaps.next();
//            String name = (String) dbMap.get("name");
//            long count = (Long) dbMap.get("count");
//            String createdAt = (String) dbMap.get("created_at");
//            String updatedAt = (String) dbMap.get("updated_at");
//            databases.add(new DatabaseSummary(name, count, createdAt, updatedAt));
//        }
//
//        // session
//        return new ListFilesResult(session);
    }

    @Override
    public CreateSessionResult createSession(CreateSessionRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.checkCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_CREATE,
                    e(request.getSessionName()),
                    e(request.getDatabaseName()),
                    e(request.getTableName()));
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
        Map<String, String> map = (Map<String, String>) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = map.get("name");
        if (!sessName.equals(request.getDatabaseName())) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new CreateSessionResult(request.getSession());
    }

    @Override
    public UploadFileResult uploadFile(UploadFileRequest request)
            throws ClientException {
        return null;
//        // TODO #MN rename uploadFile to uploadData
//        request.setCredentials(clientAdaptor.getConfig().getCredentials());
//        validator.checkCredentials(clientAdaptor, request);
//
//        String jsonData = null;
//        try {
//            conn = createConnection();
//
//            // send request
//            String path = String.format(HttpURL.V3_UPLOAD_PART,
//                    e(request.getSessionName()),
//                    e(request.getFileID()));
//            conn.doPutRequest(request, path, request.getBytes());
//
//            // receive response code
//            int code = conn.getResponseCode();
//            if (code != HttpURLConnection.HTTP_OK) {
//                String msg = String.format("Import data failed (%s (%d): %s)",
//                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
//                LOG.severe(msg);
//                throw new ClientException(msg);
//            }
//
//            // receive response body
//            jsonData = conn.getResponseBody();
//            validator.validateJSONData(jsonData);
//        } catch (IOException e) {
//            throw new ClientException(e);
//        } finally {
//            if (conn != null) {
//                conn.disconnect();
//            }
//        }
//
//        // parse JSON data
//        @SuppressWarnings("unchecked")
//        Map<String, Object> map = (Map<String, Object>) JSONValue.parse(jsonData);
//        validator.validateJavaObject(jsonData, map);
//
//        // TODO session
//        return new UploadFileResult(session);
    }

    @Override
    public DeleteFileResult deleteFile(DeleteFileRequest request)
            throws ClientException {
        return null;
//        // TODO #MN rename deleteFile to deleteData
//        request.setCredentials(clientAdaptor.getConfig().getCredentials());
//        validator.checkCredentials(clientAdaptor, request);
//
//        String jsonData = null;
//        try {
//            conn = createConnection();
//
//            // send request
//            String path = String.format(HttpURL.V3_DELETE_PART,
//                    e(request.getSessionName()),
//                    e(request.getFileID()));
//            Map<String, String> header = null;
//            Map<String, String> params = null;
//            conn.doPostRequest(request, path, header, params);
//
//            // receive response code
//            int code = conn.getResponseCode();
//            if (code != HttpURLConnection.HTTP_OK) {
//                String msg = String.format("Delete table failed (%s (%d): %s)",
//                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
//                LOG.severe(msg);
//                throw new ClientException(msg);
//            }
//
//            // receive response body
//            jsonData = conn.getResponseBody();
//            validator.validateJSONData(jsonData);
//        } catch (IOException e) {
//            throw new ClientException(e);
//        } finally {
//            if (conn != null) {
//                conn.disconnect();
//            }
//        }
//
//        // parse JSON data
//        @SuppressWarnings("unchecked")
//        Map<String, String> tableMap = (Map<String, String>) JSONValue.parse(jsonData);
//        validator.validateJavaObject(jsonData, tableMap);
//
//        // TODO session
//        return new DeleteFileResult(session);
    }

    @Override
    public PerformSessionResult performSession(PerformSessionRequest request)
            throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetErrorRecordsResult getErrorRecords(GetErrorRecordsRequest request)
            throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CommitSessionResult commitSession(CommitSessionRequest request)
            throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteSessionResult deleteSession(DeleteSessionRequest request)
            throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FreezeSessionResult freezeSession(FreezeSessionRequest request)
            throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnfreezeSessionResult unfreezeSession(UnfreezeSessionRequest request)
            throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

}
