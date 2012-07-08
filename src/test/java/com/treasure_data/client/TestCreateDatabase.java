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
import com.treasure_data.client.HttpConnectionImpl;
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.Request;

public class TestCreateDatabase {
    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void testCreateDatabase00() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);

        String databaseName = "db1";
        try {
            CreateDatabaseRequest request = new CreateDatabaseRequest(databaseName);
            CreateDatabaseResult result = clientAdaptor.createDatabase(request);
            System.out.println(result.getDatabase().getName());
        } finally {
            DeleteDatabaseRequest request = new DeleteDatabaseRequest(databaseName);
            clientAdaptor.deleteDatabase(request);
        }
    }

    static class HttpConnectionImplforCreateDatabase01 extends HttpConnectionImpl {
        @Override
        public void doPostRequest(Request<?> request, String path, Map<String, String> header,
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
            Map<String, String> map = new HashMap<String, String>();
            map.put("database", "testdb");
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
    public void testCreateDatabase01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforCreateDatabase01());

        String databaseName = "testdb";
        CreateDatabaseRequest request = new CreateDatabaseRequest(databaseName);
        CreateDatabaseResult result = clientAdaptor.createDatabase(request);
        assertEquals(databaseName, result.getDatabase().getName());
    }

    static class HttpConnectionImplforCreateDatabase02 extends HttpConnectionImpl {
        @Override
        public void doPostRequest(Request<?> request, String path, Map<String, String> header,
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
    public void testCreateDatabase02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforCreateDatabase02());

        String databaseName = "testdb";
        CreateDatabaseRequest request = new CreateDatabaseRequest(databaseName);
        try {
            clientAdaptor.createDatabase(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforCreateDatabase03 extends HttpConnectionImpl {
        @Override
        public void doPostRequest(Request<?> request, String path, Map<String, String> header,
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
    public void testCreateDatabase03() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforCreateDatabase03());

        String databaseName = "testdb";
        CreateDatabaseRequest request = new CreateDatabaseRequest(databaseName);
        try {
            clientAdaptor.createDatabase(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
