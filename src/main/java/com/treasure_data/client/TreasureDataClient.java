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

import java.util.Properties;
import java.util.logging.Logger;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.DeleteDatabaseResult;
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.DeleteTableResult;
import com.treasure_data.model.ExportRequest;
import com.treasure_data.model.ExportResult;
import com.treasure_data.model.ImportRequest;
import com.treasure_data.model.ImportResult;
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

public class TreasureDataClient {
    private static Logger LOG = Logger.getLogger(TreasureDataClient.class.getName());

    /**
     * adaptor factory method
     */
    static ClientAdaptor createClientAdaptor(
	    TreasureDataCredentials credentials, Properties props) {
	Config conf = new Config();
	conf.setCredentials(credentials);
	ClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
	return clientAdaptor;
    }

    private ClientAdaptor clientAdaptor;

    public TreasureDataClient() {
        this(System.getProperties());
    }

    public TreasureDataClient(Properties props) {
        this(new TreasureDataCredentials(props), props);
    }

    public TreasureDataClient(TreasureDataCredentials credentials, Properties props) {
	this.clientAdaptor = createClientAdaptor(credentials, props);
    }

    // Server Status API

    public ServerStatusResult getServerStatus() throws ClientException {
        return getServerStatus(new ServerStatusRequest());
    }

    public ServerStatusResult getServerStatus(ServerStatusRequest request)
            throws ClientException {
        return clientAdaptor.getServerStatus(request);
    }

    // Database API

    public ListDatabasesResult listDatabases() throws ClientException {
        return listDatabases(new ListDatabasesRequest());
    }

    public ListDatabasesResult listDatabases(ListDatabasesRequest request)
            throws ClientException {
        return clientAdaptor.listDatabases(request);
    }

    public CreateDatabaseResult createDatabase(CreateDatabaseRequest request)
            throws ClientException {
	return clientAdaptor.createDatabase(request);
    }

    public DeleteDatabaseResult deleteDatabase(DeleteDatabaseRequest request)
            throws ClientException {
	return clientAdaptor.deleteDatabase(request);
    }

    // Table API

    public ListTablesResult listTables(ListTablesRequest request) throws ClientException {
	return clientAdaptor.listTables(request);
    }

    public CreateTableResult createTable(CreateTableRequest request)
            throws ClientException {
	return clientAdaptor.createTable(request);
    }

    public DeleteTableResult deleteTable(DeleteTableRequest request)
            throws ClientException {
	return clientAdaptor.deleteTable(request);
    }

    // Import and Export API

    public ImportResult importData(ImportRequest request) throws ClientException {
        return clientAdaptor.importData(request);
    }

    public ExportResult exportData(ExportRequest request) throws ClientException {
        return clientAdaptor.exportData(request);
    }

    // Job API

    public SubmitJobResult submitJob(SubmitJobRequest request) throws ClientException {
        return clientAdaptor.submitJob(request);
    }

    public ListJobsResult listJobs(ListJobsRequest request) throws ClientException {
        return clientAdaptor.listJobs(request);
    }

    public KillJobResult killJob(KillJobRequest request) throws ClientException {
        return clientAdaptor.killJob(request);
    }

    public ShowJobResult showJob(ShowJobRequest request) throws ClientException {
        return clientAdaptor.showJob(request);
    }

    public GetJobResultResult getJobResult(GetJobResultRequest request) throws ClientException {
        return clientAdaptor.getJobResult(request);
    }
}
