package com.treasure_data.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpConnectionImpl;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.ListJobsRequest;
import com.treasure_data.model.ListJobsResult;
import com.treasure_data.model.Request;

public class TestListJobs {

    @Test @Ignore
    public void testListJobs00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        ListJobsRequest request = new ListJobsRequest();
        ListJobsResult result = clientAdaptor.listJobs(request);
        List<JobSummary> jobs = result.getJobs();
        for (JobSummary job : jobs) {
            System.out.println(job.getJobID());
        }
    }

    static class HttpConnectionImplforListJobs01 extends HttpConnectionImpl {
        @Override
        public void doGetRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            // do nothing
        }

        @Override
        public int getResponseCode() throws IOException {
            return HttpURLConnection.HTTP_OK;
        }

        @Override
        public String getResponseMessage() throws IOException {
            return "";
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public String getResponseBody() throws IOException {
            Map map = new HashMap();
            map.put("count", 3);
            map.put("from", 30);
            map.put("to", 32);
            List jobs = new ArrayList();
            Map j0 = new HashMap();
            j0.put("type", "hive");
            j0.put("job_id", "12345");
            j0.put("status", "running");
            j0.put("created_at", "2011-09-09 05:31:00 UTC");
            j0.put("start_at", "2011-09-09 05:31:10 UTC");
            j0.put("end_at", "2011-09-09 05:59:19 UTC");
            j0.put("query", "SELECT * FROM access");
            j0.put("result", "output1");
            jobs.add(j0);
            Map j1 = new HashMap();
            j1.put("type", "mapred");
            j1.put("job_id", "12346");
            j1.put("status", "success");
            j1.put("created_at", "2011-09-09 05:31:00 UTC");
            j1.put("start_at", "2011-09-09 05:31:10 UTC");
            j1.put("end_at", "2011-09-09 05:59:19 UTC");
            j1.put("query", "SELECT * FROM access");
            j1.put("result", "output1");
            jobs.add(j1);
            Map j2 = new HashMap();
            j2.put("type", "pig");
            j2.put("job_id", "12347");
            j2.put("status", "failed");
            j2.put("created_at", "2011-09-09 05:31:00 UTC");
            j2.put("start_at", "2011-09-09 05:31:10 UTC");
            j2.put("end_at", "2011-09-09 05:59:19 UTC");
            j2.put("query", "SELECT * FROM access");
            j2.put("result", "output1");
            jobs.add(j2);
            map.put("jobs", jobs);
            String jsonData = JSONValue.toJSONString(map);
            return jsonData;
        }

        @Override
        public void disconnect() {
            // do nothing
        }
    }

    /**
     * check normal behavior of client
     */
    @Test
    public void testListDatabases01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListJobs01());

        ListJobsRequest request = new ListJobsRequest();
        ListJobsResult result = clientAdaptor.listJobs(request);
        List<JobSummary> jobs = result.getJobs();

        // confirm
        List<String> srcJobIDs = new ArrayList<String>();
        srcJobIDs.add("12345");
        srcJobIDs.add("12346");
        srcJobIDs.add("12347");
        for (Job job : jobs) {
            assertTrue(srcJobIDs.contains(job.getJobID()));
        }
    }

    static class HttpConnectionImplforListJobs02 extends HttpConnectionImpl {
        @Override
        public void doGetRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            // do nothing
        }

        @Override
        public int getResponseCode() throws IOException {
            return HttpURLConnection.HTTP_OK;
        }

        @Override
        public String getResponseMessage() throws IOException {
            return "";
        }

        @Override
        public String getResponseBody() throws IOException {
            return "foobar"; // invalid JSON data
        }

        @Override
        public void disconnect() {
            // do nothing
        }
    }

    /**
     * check behavior when receiving *invalid JSON data* as response body
     */
    @Test
    public void testListDatabases02() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListJobs02());

        try {
            ListJobsRequest request = new ListJobsRequest();
            clientAdaptor.listJobs(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforListJobs03 extends HttpConnectionImpl {
        @Override
        public void doGetRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            // do nothing
        }

        @Override
        public int getResponseCode() throws IOException {
            return HttpURLConnection.HTTP_BAD_REQUEST;
        }

        @Override
        public String getResponseMessage() throws IOException {
            return "";
        }

        @Override
        public String getResponseBody() throws IOException {
            return "";
        }

        @Override
        public void disconnect() {
            // do nothing
        }
    }

    /**
     * check behavior when receiving non-OK response code
     */
    @Test
    public void testListDatabases03() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListJobs03());

        try {
            ListJobsRequest request = new ListJobsRequest();
            clientAdaptor.listJobs(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
