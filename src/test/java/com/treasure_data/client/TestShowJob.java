package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpClientAdaptor.HttpConnectionImpl;
import com.treasure_data.model.Job;
import com.treasure_data.model.Request;
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;

public class TestShowJob {
    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void testShowJob00() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);

        ShowJobRequest request = new ShowJobRequest(new Job("200338"));
        ShowJobResult result = clientAdaptor.showJob(request);
        System.out.println(result.getJob().getJobID());
        System.out.println(result.getJob().getStatus());
        System.out.println(result.getJob().getResultSchema());
    }

    static class HttpConnectionImplforShowJob01 extends HttpConnectionImpl {
        @Override
        void doGetRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            // do nothing
        }

        @Override
        int getResponseCode() throws IOException {
            return HttpURLConnection.HTTP_OK;
        }

        @Override
        String getResponseMessage() throws IOException {
            return "";
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        String getResponseBody() throws IOException {
            Map map = new HashMap();
            map.put("type", "hive");
            map.put("query", "SELECT * FROM ACCESS");
            map.put("result", "output1");
            map.put("job_id", "12345");
            map.put("status", "success");
            map.put("status_str", "");
            map.put("url", "http://console.treasure-data.com/jobs/12345");
            map.put("created_at", "Sun Jun 26 17:39:18 -0400 2011");
            map.put("updated_at", "Sun Jun 26 17:39:54 -0400 2011");
            String jsonData = JSONValue.toJSONString(map);
            return jsonData;
        }

        @Override
        void disconnect() {
            // do nothing
        }
    }

    /**
     * check normal behavior of client
     */
    @Test
    public void testShowJob01() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforShowJob01());

        String jobID = "12345";
        ShowJobRequest request = new ShowJobRequest(new Job(jobID));
        ShowJobResult result = clientAdaptor.showJob(request);
        assertEquals(jobID, result.getJob().getJobID());
        assertEquals(Job.Type.HIVE, result.getJob().getType());
        assertEquals(Job.Status.SUCCESS, result.getJob().getStatus());
    }

    static class HttpConnectionImplforShowJob02 extends HttpConnectionImpl {
        @Override
        void doGetRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            // do nothing
        }

        @Override
        int getResponseCode() throws IOException {
            return HttpURLConnection.HTTP_OK;
        }

        @Override
        String getResponseMessage() throws IOException {
            return "";
        }

        @Override
        String getResponseBody() throws IOException {
            return "foobar"; // invalid JSON data
        }

        @Override
        void disconnect() {
            // do nothing
        }
    }

    /**
     * check behavior when receiving *invalid JSON data* as response body
     */
    @Test
    public void testShowJob02() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforShowJob02());

        try {
            ShowJobRequest request = new ShowJobRequest(new Job("26597"));
            clientAdaptor.showJob(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforShowJob03 extends HttpConnectionImpl {
        @Override
        void doGetRequest(Request<?> request, String path, Map<String, String> header,
                Map<String, String> params) throws IOException {
            // do nothing
        }

        @Override
        int getResponseCode() throws IOException {
            return HttpURLConnection.HTTP_BAD_REQUEST;
        }

        @Override
        String getResponseMessage() throws IOException {
            return "";
        }

        @Override
        String getResponseBody() throws IOException {
            return "";
        }

        @Override
        void disconnect() {
            // do nothing
        }
    }

    /**
     * check behavior when receiving non-OK response code
     */
    @Test
    public void testShowJob03() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforShowJob03());

        try {
            ShowJobRequest request = new ShowJobRequest(new Job("26597"));
            clientAdaptor.showJob(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
