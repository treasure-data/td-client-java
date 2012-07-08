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

import java.util.List;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.bulkimport.CommitSessionRequest;
import com.treasure_data.model.bulkimport.CommitSessionResult;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeleteFileRequest;
import com.treasure_data.model.bulkimport.DeleteFileResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.ErrorRecords;
import com.treasure_data.model.bulkimport.FreezeSessionRequest;
import com.treasure_data.model.bulkimport.FreezeSessionResult;
import com.treasure_data.model.bulkimport.GetErrorRecordsRequest;
import com.treasure_data.model.bulkimport.GetErrorRecordsResult;
import com.treasure_data.model.bulkimport.ListFilesRequest;
import com.treasure_data.model.bulkimport.ListFilesResult;
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

public class BulkImportClient {

    private BulkImportClientAdaptor clientAdaptor;

    public BulkImportClient(TreasureDataClient client) {
        clientAdaptor = new BulkImportClientAdaptorImpl(client.getClientAdaptor());
    }

    /**
     * List bulk import sessions.
     *
     * @sess td command: bulk_import:list
     */
    public List<SessionSummary> listSessions() throws ClientException {
        return listSessions(new ListSessionsRequest()).getSessions();
    }

    public ListSessionsResult listSessions(ListSessionsRequest request) throws ClientException {
        return clientAdaptor.listSessions(request);
    }

    /**
     * Show a list of uploaded files.
     *
     * @param sess
     * @return
     * @throws ClientException
     * @see td command: bulk_import:show <name>
     */
    public List<String> listFiles(Session sess) throws ClientException {
        return listFiles(new ListFilesRequest(sess)).getUploadedFiles();
    }

    public ListFilesResult listFiles(ListFilesRequest request) throws ClientException {
        return clientAdaptor.listFiles(request);
    }

    /**
     * Create a new bulk import session to the table.
     *
     * @param sessName
     * @param databaseName
     * @param tableName
     * @return
     * @throws ClientException
     * @see td command: bulk_import:create <name> <db> <tbl>
     */
    public Session createSession(String sessName, String databaseName, String tableName)
            throws ClientException {
        return createSession(new CreateSessionRequest(
                sessName, databaseName, tableName)).getSession();
    }

    public CreateSessionResult createSession(CreateSessionRequest request) throws ClientException {
        return clientAdaptor.createSession(request);
    }

    /**
     * Upload or re-upload a file into a bulk import session.
     *
     * @param sess
     * @param fileID
     * @param fileName  path.msgpack.gz
     * @throws ClientException
     * @see td command: bulk_import:upload_part <name> <id> <path.msgpack.gz>
     */
    public void uploadFile(Session sess, String fileID, byte[] bytes) throws ClientException {
        uploadFile(new UploadFileRequest(sess, fileID, bytes));
    }

    public UploadFileResult uploadFile(UploadFileRequest request) throws ClientException {
        return clientAdaptor.uploadFile(request);
    }

    /**
     * Delete an uploaded file from a bulk iport session.
     *
     * @param sess
     * @param fileID
     * @throws ClientException
     * @see td command: bulk_import:delete_part <name> <id>
     */
    public void deleteFile(Session sess, String fileID) throws ClientException {
        deleteFile(new DeleteFileRequest(sess, fileID));
    }

    public DeleteFileResult deleteFile(DeleteFileRequest request)
            throws ClientException {
        return clientAdaptor.deleteFile(request);
    }

    /**
     * Start to validate and convert uploaded files.
     *
     * @param sess
     * @throws ClientException
     * @see td command: bulk_import:perform <name>
     */
    public void performSession(Session sess) throws ClientException {
        performSession(new PerformSessionRequest(sess));
    }

    public PerformSessionResult performSession(PerformSessionRequest request)
            throws ClientException {
        return clientAdaptor.performSession(request);
    }

    /**
     * Show records which did not pass validations.
     *
     * @see td command: bulk_import:error_records <name>
     */
    public ErrorRecords getErrorRecords(Session sess) throws ClientException {
        return getErrorRecords(new GetErrorRecordsRequest(sess)).getErrorRecords();
    }

    public GetErrorRecordsResult getErrorRecords(GetErrorRecordsRequest request)
            throws ClientException {
        return clientAdaptor.getErrorRecords(request);
    }

    /**
     * Start to commit a performed bulk import session.
     *
     * @param sess
     * @throws ClientException
     * @see td command: bulk_import:commit <name>
     */
    public void commitSession(Session sess) throws ClientException {
        commitSession(new CommitSessionRequest(sess));
    }

    public CommitSessionResult commitSession(CommitSessionRequest request)
            throws ClientException {
        return clientAdaptor.commitSession(request);
    }

    /**
     * Delete a bulk import session.
     *
     * @param sess
     * @throws ClientException
     * @see td command: bulk_import:delete <name>
     */
    public void deleteSession(Session sess) throws ClientException {
        deleteSession(new DeleteSessionRequest(sess));
    }

    public DeleteSessionResult deleteSession(DeleteSessionRequest request)
            throws ClientException {
        return clientAdaptor.deleteSession(request);
    }

    /**
     * Freeze a bulk import session. The method enables to reject succeeding
     * uploadings to a bulk import session.
     *
     * @param sess
     * @throws ClientException
     * @see td command: bulk_import:freeze <name>
     */
    public void freezeSession(Session sess) throws ClientException {
        freezeSession(new FreezeSessionRequest(sess));
    }

    public FreezeSessionResult freezeSession(FreezeSessionRequest request)
            throws ClientException {
        return clientAdaptor.freezeSession(request);
    }

    /**
     * Unfreeze a frozen bulk import session.
     *
     * @param sess
     * @throws ClientException
     * @see td command: bulk_import:unfreeze <name>
     */
    public void unfreezeSession(Session sess) throws ClientException {
        unfreezeSession(new UnfreezeSessionRequest(sess));
    }

    public UnfreezeSessionResult unfreezeSession(UnfreezeSessionRequest request)
            throws ClientException {
        return clientAdaptor.unfreezeSession(request);
    }
}
