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
