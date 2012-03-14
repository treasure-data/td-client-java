package com.treasure_data.client;

import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpClientAdaptor.HttpConnectionImpl;
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.DeleteDatabaseResult;
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.DeleteTableResult;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.KillJobRequest;
import com.treasure_data.model.KillJobResult;
import com.treasure_data.model.ListDatabasesRequest;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.ListJobsRequest;
import com.treasure_data.model.ListJobsResult;
import com.treasure_data.model.ListTablesRequest;
import com.treasure_data.model.ListTablesResult;
import com.treasure_data.model.ServerStatusRequest;
import com.treasure_data.model.ServerStatusResult;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.Table;

public class TestHttpClientAdaptor {

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(TestTreasureDataClient.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    static class HttpConnectionImplforGetServerStatus01 extends HttpConnectionImpl {
        
    }

    @Test
    public void testGetServerStatus01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforGetServerStatus01());
        ServerStatusRequest request = new ServerStatusRequest();
        ServerStatusResult result = clientAdaptor.getServerStatus(request);
        System.out.println(result.getMessage());
    }

    @Test @Ignore
    public void testListDatabases01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        ListDatabasesRequest request = new ListDatabasesRequest();
        ListDatabasesResult result = clientAdaptor.listDatabases(request);
        List<Database> databases = result.getDatabases();
        for (Database database : databases) {
            System.out.println(database.getName());
        }
    }

    @Test @Ignore
    public void testCreateDatabase01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        CreateDatabaseRequest request = new CreateDatabaseRequest("test_http_client_adaptor");
        CreateDatabaseResult result = clientAdaptor.createDatabase(request);
        System.out.println(result.getDatabase().getName());
    }

    @Test @Ignore
    public void testDeleteDatabase01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        DeleteDatabaseRequest request = new DeleteDatabaseRequest(new Database("test_http_client_adaptor"));
        DeleteDatabaseResult result = clientAdaptor.deleteDatabase(request);
        System.out.println(result.getDatabaseName());
    }

    @Test @Ignore
    public void testListTables01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        //ListTablesRequest request = new ListTablesRequest(new Database("test_merge_0"));
        ListTablesRequest request = new ListTablesRequest();
        ListTablesResult result = clientAdaptor.listTables(request);
        List<Table> tables = result.getTables();
        for (Table table : tables) {
            System.out.println(table.getDatabase().getName());
            System.out.println(table.getName());
            System.out.println(table.getType());
            System.out.println(table.getCount());
        }
    }

    @Test @Ignore
    public void testCreateTable01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        try {
            // create database
            CreateDatabaseResult ret =
                clientAdaptor.createDatabase(new CreateDatabaseRequest("test_http_client_adaptor"));
            Database database = ret.getDatabase();

            CreateTableRequest request = new CreateTableRequest(database, "test01", Table.Type.LOG);
            CreateTableResult result = clientAdaptor.createTable(request);
            System.out.println(result.getTable().getName());
        } finally {
            // delete database
            clientAdaptor.deleteDatabase(new DeleteDatabaseRequest(new Database("test_http_client_adaptor")));
        }
    }

    @Test @Ignore
    public void testDeleteTable01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        try {
            // create database
            CreateDatabaseResult ret =
                clientAdaptor.createDatabase(new CreateDatabaseRequest("test_http_client_adaptor"));
            Database database = ret.getDatabase();
            CreateTableRequest req = new CreateTableRequest(database, "test01", Table.Type.LOG);
            CreateTableResult res = clientAdaptor.createTable(req);
            Table table = res.getTable();

            DeleteTableRequest request = new DeleteTableRequest(table);
            DeleteTableResult result = clientAdaptor.deleteTable(request);
            System.out.println(result.getTableName());
        } finally {
            // delete database
            clientAdaptor.deleteDatabase(new DeleteDatabaseRequest(new Database("test_http_client_adaptor")));
        }
    }

    @Test @Ignore
    public void testSubmitJob01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        Database database = new Database("mugadb");
        String q = "select * from mugatbl";
        SubmitJobRequest request = new SubmitJobRequest(new Job(null, null, database, q, null));
        SubmitJobResult result = clientAdaptor.submitJob(request);
        Job job = result.getJob();
        System.out.println(job.getJobID());
    }

    @Test @Ignore
    public void testListJobs01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        ListJobsRequest request = new ListJobsRequest();
        ListJobsResult result = clientAdaptor.listJobs(request);
        List<Job> jobs = result.getJobs();
        for (Job job : jobs) {
            System.out.println(job.getJobID());    
        }
    }

    @Test @Ignore
    public void testKillJob01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        KillJobRequest request = new KillJobRequest(new Job("25773", "hive"));
        KillJobResult result = clientAdaptor.killJob(request);
        System.out.println(result.getJobID());
    }

    @Test @Ignore
    public void testShowJob01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        ShowJobRequest request = new ShowJobRequest(new Job("26597"));
        ShowJobResult result = clientAdaptor.showJob(request);
        System.out.println(result.getJob().getJobID());
        System.out.println(result.getJob().getStatus());
    }

    @Test @Ignore
    public void testGetJobResult01() throws Exception {
        Properties props = System.getProperties();
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        GetJobResultRequest request = new GetJobResultRequest(
                new JobResult(new Job("26317"), JobResult.Format.MSGPACK));
        GetJobResultResult result = clientAdaptor.getJobResult(request);
        System.out.println(result.getJob().getJobID());
    }
}
