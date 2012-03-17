package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
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
import com.treasure_data.model.Request;
import com.treasure_data.model.ServerStatusRequest;
import com.treasure_data.model.ServerStatusResult;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.Table;

public class TestHttpClientAdaptor {
    @Test
    public void testSetTreasureDataCredentials() throws Exception {
        Config conf = new Config();
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);

        {
            TreasureDataCredentials credentails = clientAdaptor.getTreasureDataCredentials();
            assertTrue(credentails == null);
        }

        {
            Properties props = new Properties();
            props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
            clientAdaptor.setTreasureDataCredentials(new TreasureDataCredentials(props));
            TreasureDataCredentials credentails = clientAdaptor.getTreasureDataCredentials();
            assertTrue(credentails != null);
            assertTrue(credentails.getAPIKey() != null);
        }
    }

    // TODO


    @Test @Ignore
    public void testDeleteTable01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        //clientAdaptor.setConnection(new MockHttpConnectionImpl0());

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
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        //clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        Database database = new Database("mugadb");
        String q = "select * from mugatbl";
        SubmitJobRequest request = new SubmitJobRequest(new Job(null, null, database, q, null));
        SubmitJobResult result = clientAdaptor.submitJob(request);
        Job job = result.getJob();
        System.out.println(job.getJobID());
    }

    @Test @Ignore
    public void testListJobs01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        //clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        ListJobsRequest request = new ListJobsRequest();
        ListJobsResult result = clientAdaptor.listJobs(request);
        List<Job> jobs = result.getJobs();
        for (Job job : jobs) {
            System.out.println(job.getJobID());    
        }
    }

    @Test @Ignore
    public void testKillJob01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        //clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        KillJobRequest request = new KillJobRequest(new Job("25773", Job.Type.HIVE));
        KillJobResult result = clientAdaptor.killJob(request);
        System.out.println(result.getJobID());
    }

    @Test @Ignore
    public void testShowJob01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        //clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        ShowJobRequest request = new ShowJobRequest(new Job("26597"));
        ShowJobResult result = clientAdaptor.showJob(request);
        System.out.println(result.getJob().getJobID());
        System.out.println(result.getJob().getStatus());
    }

    @Test @Ignore
    public void testGetJobResult01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        //clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        GetJobResultRequest request = new GetJobResultRequest(
                new JobResult(new Job("26317"), JobResult.Format.MSGPACK));
        GetJobResultResult result = clientAdaptor.getJobResult(request);
        System.out.println(result.getJob().getJobID());
    }
}
