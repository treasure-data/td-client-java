package com.treasure_data.client.bulkimport;

import com.treasure_data.client.ClientException;
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
import com.treasure_data.model.bulkimport.ListSessionsRequest;
import com.treasure_data.model.bulkimport.ListSessionsResult;
import com.treasure_data.model.bulkimport.PerformSessionRequest;
import com.treasure_data.model.bulkimport.PerformSessionResult;
import com.treasure_data.model.bulkimport.UnfreezeSessionRequest;
import com.treasure_data.model.bulkimport.UnfreezeSessionResult;
import com.treasure_data.model.bulkimport.UploadFileRequest;
import com.treasure_data.model.bulkimport.UploadFileResult;

public interface BulkImportClientAdaptor {

    ListSessionsResult listSessions(ListSessionsRequest request) throws ClientException;

    ListFilesResult listFiles(ListFilesRequest request) throws ClientException;

    CreateSessionResult createSession(CreateSessionRequest request) throws ClientException;

    UploadFileResult uploadFile(UploadFileRequest request) throws ClientException;

    DeleteFileResult deleteFile(DeleteFileRequest request) throws ClientException;

    PerformSessionResult performSession(PerformSessionRequest request) throws ClientException;

    GetErrorRecordsResult getErrorRecords(GetErrorRecordsRequest request) throws ClientException;

    CommitSessionResult commitSession(CommitSessionRequest request) throws ClientException;

    DeleteSessionResult deleteSession(DeleteSessionRequest request) throws ClientException;

    FreezeSessionResult freezeSession(FreezeSessionRequest request) throws ClientException;

    UnfreezeSessionResult unfreezeSession(UnfreezeSessionRequest request) throws ClientException;
}
