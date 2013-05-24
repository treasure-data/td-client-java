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
package com.treasure_data.client;

import java.util.List;
import java.util.Properties;

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
import com.treasure_data.model.ListDatabasesRequest;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.ListJobsRequest;
import com.treasure_data.model.ListJobsResult;
import com.treasure_data.model.ListTablesRequest;
import com.treasure_data.model.ListTablesResult;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.RenameTableRequest;
import com.treasure_data.model.RenameTableResult;
import com.treasure_data.model.ServerStatus;
import com.treasure_data.model.GetServerStatusRequest;
import com.treasure_data.model.GetServerStatusResult;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.SwapTableRequest;
import com.treasure_data.model.SwapTableResult;
import com.treasure_data.model.Table;
import com.treasure_data.model.TableSummary;

public class TreasureDataClient {
    /**
     * adaptor factory method
     */
    static DefaultClientAdaptor createClientAdaptor(
            TreasureDataCredentials credentials, Properties props) {
        Config conf = new Config(props);
        conf.setCredentials(credentials);
        DefaultClientAdaptor clientAdaptor = new DefaultClientAdaptorImpl(conf);
        return clientAdaptor;
    }

    private DefaultClientAdaptor clientAdaptor;

    public TreasureDataClient() {
        this(System.getProperties());
    }

    public TreasureDataClient(Properties props) {
        this(new TreasureDataCredentials(props), props);
    }

    public TreasureDataClient(TreasureDataCredentials credentials,
            Properties props) {
        clientAdaptor = createClientAdaptor(credentials, props);
    }

    public Config getConfig() {
        return clientAdaptor.getConfig();
    }

    public DefaultClientAdaptor getClientAdaptor() {
        return clientAdaptor;
    }

    public TreasureDataCredentials getTreasureDataCredentials() {
        return clientAdaptor.getTreasureDataCredentials();
    }

    public void setTreasureDataCredentials(TreasureDataCredentials credentials) {
        clientAdaptor.setTreasureDataCredentials(credentials);
    }

    public void authenticate(String email, String password)
            throws ClientException {
        authenticate(new AuthenticateRequest(email, password));
    }

    public AuthenticateResult authenticate(AuthenticateRequest request)
            throws ClientException {
        AuthenticateResult result = clientAdaptor.authenticate(request);
        TreasureDataCredentials credentials = result
                .getTreasureDataCredentials();
        setTreasureDataCredentials(credentials);
        return result;
    }

    // Server Status API

    public ServerStatus getServerStatus() throws ClientException {
        return clientAdaptor.getServerStatus(new GetServerStatusRequest())
                .getServerStatus();
    }

    public GetServerStatusResult getServerStatus(GetServerStatusRequest request)
            throws ClientException {
        return clientAdaptor.getServerStatus(new GetServerStatusRequest());
    }

    // Database API

    public List<DatabaseSummary> listDatabases() throws ClientException {
        return listDatabases(new ListDatabasesRequest()).getDatabases();
    }

    public ListDatabasesResult listDatabases(ListDatabasesRequest request)
            throws ClientException {
        return clientAdaptor.listDatabases(request);
    }

    public Database createDatabase(String databaseName) throws ClientException {
        return createDatabase(new CreateDatabaseRequest(databaseName))
                .getDatabase();
    }

    public CreateDatabaseResult createDatabase(CreateDatabaseRequest request)
            throws ClientException {
        return clientAdaptor.createDatabase(request);
    }

    public void deleteDatabase(String databaseName) throws ClientException {
        deleteDatabase(new DeleteDatabaseRequest(new Database(databaseName)));
    }

    public DeleteDatabaseResult deleteDatabase(DeleteDatabaseRequest request)
            throws ClientException {
        return clientAdaptor.deleteDatabase(request);
    }

    // Table API

    public List<TableSummary> listTables(String databaseName)
            throws ClientException {
        return listTables(new Database(databaseName));
    }

    public List<TableSummary> listTables(Database database)
            throws ClientException {
        return listTables(new ListTablesRequest(database)).getTables();
    }

    public ListTablesResult listTables(ListTablesRequest request)
            throws ClientException {
        return clientAdaptor.listTables(request);
    }

    public Table createTable(String databaseName, String tableName)
            throws ClientException {
        return createTable(new Database(databaseName), tableName);
    }

    public Table createTable(Database database, String tableName)
            throws ClientException {
        CreateTableResult result = createTable(new CreateTableRequest(database,
                tableName));
        return result.getTable();
    }

    public void renameTable(String databaseName, String origTableName,
            String newTableName) throws ClientException {
        renameTable(new RenameTableRequest(databaseName, origTableName, newTableName));
    }

    public RenameTableResult renameTable(RenameTableRequest request) throws ClientException {
        return clientAdaptor.renameTable(request);
    }

    public void swapTable(String databaseName, String tableName1,
            String tableName2) throws ClientException {
        swapTable(new SwapTableRequest(databaseName, tableName1, tableName2));
    }

    public SwapTableResult swapTable(SwapTableRequest request)
            throws ClientException {
        return clientAdaptor.swapTable(request);
    }

    public CreateTableResult createTable(CreateTableRequest request)
            throws ClientException {
        return clientAdaptor.createTable(request);
    }

    public void deleteTable(String databaseName, String tableName)
            throws ClientException {
        deleteTable(new DeleteTableRequest(new Table(
                new Database(databaseName), tableName, Table.Type.LOG)));
    }

    public DeleteTableResult deleteTable(DeleteTableRequest request)
            throws ClientException {
        return clientAdaptor.deleteTable(request);
    }

    public void deletePartiallTable(String databaseName, String tableName,
            long from, long to) throws ClientException {
        clientAdaptor.deletePartialTable(new DeletePartialTableRequest(
                new Table(new Database(databaseName), tableName), from, to));
    }

    public DeletePartialTableResult deletePartiallTable(
            DeletePartialTableRequest request) throws ClientException {
        return clientAdaptor.deletePartialTable(request);
    }

    // TODO #MN add it in next version
    //TableSchema updateTableSchema(UpdateTableSchemaRequest request) throws ClientException;

    // Import API
    public ImportResult importData(String databaseName, String tableName,
            byte[] data) throws ClientException {
        return importData(new Table(new Database(databaseName), tableName),
                data);
    }

    public ImportResult importData(Table table, byte[] data)
            throws ClientException {
        return importData(new ImportRequest(table, data));
    }

    public ImportResult importData(ImportRequest request)
            throws ClientException {
        return clientAdaptor.importData(request);
    }

    // Export API

    public ExportResult exportData(ExportRequest request)
            throws ClientException {
        return clientAdaptor.exportData(request);
    }

    // Job API

    public void submitJob(Job job) throws ClientException {
        submitJob(new SubmitJobRequest(job));
    }

    public SubmitJobResult submitJob(SubmitJobRequest request)
            throws ClientException {
        return clientAdaptor.submitJob(request);
    }

    public List<JobSummary> listJobs(long from, long to) throws ClientException {
        return listJobs(new ListJobsRequest(from, to)).getJobs();
    }

    public ListJobsResult listJobs(ListJobsRequest request)
            throws ClientException {
        return clientAdaptor.listJobs(request);
    }

    public void killJob(String jobID) throws ClientException {
        killJob(new Job(jobID));
    }

    public void killJob(Job job) throws ClientException {
        killJob(new KillJobRequest(job));
    }

    public KillJobResult killJob(KillJobRequest request) throws ClientException {
        return clientAdaptor.killJob(request);
    }

    public JobSummary showJob(String jobID) throws ClientException {
        return showJob(new Job(jobID));
    }

    public JobSummary showJob(Job job) throws ClientException {
        return showJob(new ShowJobRequest(job)).getJob();
    }

    public ShowJobResult showJob(ShowJobRequest request) throws ClientException {
        return clientAdaptor.showJob(request);
    }

    public JobResult getJobResult(Job job) throws ClientException {
        return getJobResult(new GetJobResultRequest(new JobResult(job)))
                .getJobResult();
    }

    public GetJobResultResult getJobResult(GetJobResultRequest request)
            throws ClientException {
        return clientAdaptor.getJobResult(request);
    }

    // Job Scheduling API

    // TODO #MN add it in next version
    //ListScheduledJobResult listJobSchedules(ListScheduledJobRequest request) throws ClientException; // JobScheduleSummary < JobSchedule

    // TODO #MN add it in next version
    //List<JobSchedule> listJobSchedules() throws ClientException;

    // TODO #MN add it in next version
    //JobSchedule createJobSchedule(CreateJobScheduleRequest request) throws ClientException;

    // TODO #MN add it in next version
    //void deleteJobSchedule(DeleteJobScheduleRequest request) throws ClientException;

    // TODO #MN add it in next version
    //void deleteJobSchedule(String jobScheduleName) throws ClientException;
}
