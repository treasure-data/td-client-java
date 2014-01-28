//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
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
import com.treasure_data.model.bulkimport.ListSessionsRequest;
import com.treasure_data.model.bulkimport.ListSessionsResult;
import com.treasure_data.model.bulkimport.PerformSessionRequest;
import com.treasure_data.model.bulkimport.PerformSessionResult;
import com.treasure_data.model.bulkimport.ShowSessionRequest;
import com.treasure_data.model.bulkimport.ShowSessionResult;
import com.treasure_data.model.bulkimport.UnfreezeSessionRequest;
import com.treasure_data.model.bulkimport.UnfreezeSessionResult;
import com.treasure_data.model.bulkimport.UploadPartRequest;
import com.treasure_data.model.bulkimport.UploadPartResult;

public interface BulkImportClientAdaptor {

	ShowSessionResult showSession(ShowSessionRequest request) throws ClientException;

	ListSessionsResult listSessions(ListSessionsRequest request) throws ClientException;

    ListPartsResult listParts(ListPartsRequest request) throws ClientException;

    CreateSessionResult createSession(CreateSessionRequest request) throws ClientException;

    UploadPartResult uploadPart(UploadPartRequest request) throws ClientException;

    DeletePartResult deletePart(DeletePartRequest request) throws ClientException;

    PerformSessionResult performSession(PerformSessionRequest request) throws ClientException;

    GetErrorRecordsResult getErrorRecords(GetErrorRecordsRequest request) throws ClientException;

    CommitSessionResult commitSession(CommitSessionRequest request) throws ClientException;

    DeleteSessionResult deleteSession(DeleteSessionRequest request) throws ClientException;

    FreezeSessionResult freezeSession(FreezeSessionRequest request) throws ClientException;

    UnfreezeSessionResult unfreezeSession(UnfreezeSessionRequest request) throws ClientException;
}
