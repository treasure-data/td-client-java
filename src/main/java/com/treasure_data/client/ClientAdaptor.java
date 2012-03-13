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

import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.DeleteDatabaseResult;
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.DeleteTableResult;
import com.treasure_data.model.KillJobRequest;
import com.treasure_data.model.KillJobResult;
import com.treasure_data.model.ListDatabasesRequest;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.ListJobsRequest;
import com.treasure_data.model.ListJobsResult;
import com.treasure_data.model.ListTablesRequest;
import com.treasure_data.model.ListTablesResult;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.ServerStatusRequest;
import com.treasure_data.model.ServerStatusResult;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;

public interface ClientAdaptor {
    // Server Status API

    ServerStatusResult getServerStatus(ServerStatusRequest request) throws ClientException;

    // Database API

    ListDatabasesResult listDatabases(ListDatabasesRequest request) throws ClientException;

    CreateDatabaseResult createDatabase(CreateDatabaseRequest request) throws ClientException;

    DeleteDatabaseResult deleteDatabase(DeleteDatabaseRequest request) throws ClientException;

    // Table API

    ListTablesResult listTables(ListTablesRequest request) throws ClientException;

    CreateTableResult createTable(CreateTableRequest request) throws ClientException;

    DeleteTableResult deleteTable(DeleteTableRequest request) throws ClientException;

    // Job API

    SubmitJobResult submitJob(SubmitJobRequest request) throws ClientException;

    ListJobsResult listJobs(ListJobsRequest request) throws ClientException;

    KillJobResult killJob(KillJobRequest request) throws ClientException;

    ShowJobResult showJob(ShowJobRequest request) throws ClientException;

    GetJobResultResult getJobResult(GetJobResultRequest request) throws ClientException;
}
