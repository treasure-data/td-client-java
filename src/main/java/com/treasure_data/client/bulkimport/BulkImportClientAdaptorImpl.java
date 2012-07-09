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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONValue;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

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
import com.treasure_data.model.bulkimport.DeletePartRequest;
import com.treasure_data.model.bulkimport.DeletePartResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.FreezeSessionRequest;
import com.treasure_data.model.bulkimport.FreezeSessionResult;
import com.treasure_data.model.bulkimport.GetErrorRecordsRequest;
import com.treasure_data.model.bulkimport.GetErrorRecordsResult;
import com.treasure_data.model.bulkimport.ListPartsRequest;
import com.treasure_data.model.bulkimport.ListPartsResult;
import com.treasure_data.model.bulkimport.ListSessions;
import com.treasure_data.model.bulkimport.ListSessionsRequest;
import com.treasure_data.model.bulkimport.ListSessionsResult;
import com.treasure_data.model.bulkimport.PerformSessionRequest;
import com.treasure_data.model.bulkimport.PerformSessionResult;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;
import com.treasure_data.model.bulkimport.UnfreezeSessionRequest;
import com.treasure_data.model.bulkimport.UnfreezeSessionResult;
import com.treasure_data.model.bulkimport.UploadPartRequest;
import com.treasure_data.model.bulkimport.UploadPartResult;
import com.treasure_data.model.bulkimport.SessionSummary.Status;

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

        String V3_DELETE_PART = "/v3/bulk_import/delete_part/%s/%s";

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
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = HttpURL.V3_LIST;
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doGetRequest(request, path, header, params);

            // receive response code and body
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List sessions failed (%s (%d): %s)",
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
        // {"bulk_imports":
        //   [
        //     {"name":"t01",
        //      "database":"sfdb",
        //      "table":"bi02",
        //      "status":"ready",
        //      "upload_frozen":false,
        //      "job_id":"70220",
        //      "valid_records":100,
        //      "error_records":10,
        //      "valid_parts":2,
        //      "error_parts":1},
        //     {"name":"sess01",
        //      "database":"mugadb",
        //      "table":"test04",
        //      "status":"uploading",
        //      "upload_frozen":false,
        //      "job_id":null,
        //      "valid_records":null,
        //      "error_records":null,
        //      "valid_parts":null,
        //      "error_parts":null}]}
        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        @SuppressWarnings("unchecked")
        Iterator<Map<String, Object>> sessIter =
            ((List<Map<String, Object>>) map.get("bulk_imports")).iterator();
        List<SessionSummary> sessions = new ArrayList<SessionSummary>();
        while (sessIter.hasNext()) {
            Map<String, Object> sess = sessIter.next();
            String name = (String) sess.get("name");
            String database = (String) sess.get("database");
            String table = (String) sess.get("table");
            String status = (String) sess.get("status");
            boolean upload_frozen = (Boolean) sess.get("upload_frozen");
            String job_id = (String) sess.get("job_id");
            Long vr = (Long) sess.get("valid_records");
            long valid_records = vr != null ? vr : 0;
            Long er = (Long) sess.get("error_records");
            long error_records = er != null ? er : 0;
            Long vp = (Long) sess.get("valid_parts");
            long valid_parts = vp != null ? vp : 0;
            Long ep = (Long) sess.get("error_parts");
            long error_parts = ep != null ? ep : 0;
            SessionSummary summary = new SessionSummary(name, database, table,
                    SessionSummary.toStatus(status), upload_frozen, job_id,
                    valid_records, error_records, valid_parts, error_parts);
            sessions.add(summary);
        }

        return new ListSessionsResult(new ListSessions<SessionSummary>(sessions));
    }

    @Override
    public ListPartsResult listParts(ListPartsRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_LIST_PARTS,
                    e(request.getSessionName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doGetRequest(request, path, header, params);

            // receive response code and body
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List parts failed (%s (%d): %s)",
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

        // parse JSON data {"name":"t01","parts":["error01","ok01"]}
        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = (String) map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        @SuppressWarnings("unchecked")
        List<String> parts = (List<String>) map.get("parts");
        return new ListPartsResult(request.getSession(), parts);
    }

    @Override
    public CreateSessionResult createSession(CreateSessionRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

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
                String msg = String.format("Create session failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01"}
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Map map = (Map<String, String>) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = (String) map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new CreateSessionResult(request.getSession());
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_UPLOAD_PART,
                    e(request.getSessionName()),
                    e(request.getPartID()));
            conn.doPutRequest(request, path, request.getBytes());

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Upload part failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01"}
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = (String) map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new UploadPartResult(request.getSession());
    }

    @Override
    public DeletePartResult deletePart(DeletePartRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_DELETE_PART,
                    e(request.getSessionName()),
                    e(request.getPartID()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Delete part failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01"}
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = (String) map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new DeletePartResult(request.getSession());
    }

    @Override
    public PerformSessionResult performSession(PerformSessionRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_PERFORM,
                    e(request.getSessionName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Perform session failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01","job_id":"127949"}
        @SuppressWarnings({ "rawtypes" })
        Map map = (Map) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = (String) map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }
        String job_id = (String) map.get("job_id");

        return new PerformSessionResult(request.getSession());
    }

    @Override
    public GetErrorRecordsResult getErrorRecords(GetErrorRecordsRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        Unpacker unpacker = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_ERROR_RECORDS,
                    e(request.getSessionName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doGetRequest(request, path, header, params);

            // receive response code and body
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Get error_records failed (%s (%d): %s)",
                        new Object[] { conn.getResponseMessage(), code, conn.getResponseBody() });
                LOG.severe(msg);
                throw new ClientException(msg);
            }

            // receive response body
            try {
                unpacker = getResponseBodyBinaryWithGZip(conn);
            } catch (EOFException e) {
                // ignore
            }
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        return new GetErrorRecordsResult(request.getSession(), unpacker);
    }

    private Unpacker getResponseBodyBinaryWithGZip(HttpConnectionImpl conn) throws IOException {
        GZIPInputStream in = new GZIPInputStream(conn.getInputStream());
        MessagePack msgpack = new MessagePack();
        BufferUnpacker unpacker = msgpack.createBufferUnpacker();
        byte[] buf = new byte[1024];

        int len = 0;
        while ((len = in.read(buf)) != -1) {
            unpacker.feed(buf, 0, len);
        }

        return unpacker;
    }

    @Override
    public CommitSessionResult commitSession(CommitSessionRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_COMMIT,
                    e(request.getSessionName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Commit session failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01"}
        @SuppressWarnings({ "rawtypes" })
        Map map = (Map) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = (String) map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new CommitSessionResult(request.getSession());
    }

    @Override
    public DeleteSessionResult deleteSession(DeleteSessionRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_DELETE,
                    e(request.getSessionName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Delete session failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01"}
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new DeleteSessionResult(request.getSession());
    }

    @Override
    public FreezeSessionResult freezeSession(FreezeSessionRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_FREEZE,
                    e(request.getSessionName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Freeze session failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01"}
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new FreezeSessionResult(request.getSession());
    }

    @Override
    public UnfreezeSessionResult unfreezeSession(UnfreezeSessionRequest request)
            throws ClientException {
        request.setCredentials(clientAdaptor.getConfig().getCredentials());
        validator.validateCredentials(clientAdaptor, request);

        String jsonData = null;
        try {
            conn = createConnection();

            // send request
            String path = String.format(HttpURL.V3_UNFREEZE,
                    e(request.getSessionName()));
            Map<String, String> header = null;
            Map<String, String> params = null;
            conn.doPostRequest(request, path, header, params);

            // receive response code
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Unfreeze session failed (%s (%d): %s)",
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

        // parse JSON data {"name":"sess01"}
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) JSONValue.parse(jsonData);
        validator.validateJavaObject(jsonData, map);

        String sessName = map.get("name");
        if (!request.getSessionName().equals(sessName)) {
            String msg = String.format("invalid name: expected=%s, actual=%s",
                    request.getSessionName(), sessName);
            throw new ClientException(msg);
        }

        return new UnfreezeSessionResult(request.getSession());
    }

}
