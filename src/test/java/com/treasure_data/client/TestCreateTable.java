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
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpClientAdaptor.HttpConnectionImpl;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.Request;
import com.treasure_data.model.Table;

public class TestCreateTable {
    @Before
    public void setUp() throws Exception {
    }

    static class HttpConnectionImplforCreateTable01 extends HttpConnectionImpl {
        @Override
        void doPostRequest(Request<?> request, String path, Map<String, String> header,
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
            Map<String, String> map = new HashMap<String, String>();
            map.put("database", "testdb");
            map.put("table", "testtbl");
            map.put("type", "log");
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
    public void testCreateDatabase01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforCreateTable01());

        String databaseName = "testdb";
        String tableName = "testtbl";
        CreateTableRequest request = new CreateTableRequest(new Database(databaseName), tableName, Table.Type.LOG);
        CreateTableResult result = clientAdaptor.createTable(request);
        assertEquals(databaseName, result.getDatabase().getName());
    }

    static class HttpConnectionImplforCreateTable02 extends HttpConnectionImpl {
        @Override
        void doPostRequest(Request<?> request, String path, Map<String, String> header,
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
    public void testCreateDatabase02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforCreateTable02());

        String databaseName = "testdb";
        String tableName = "testtbl";
        CreateTableRequest request = new CreateTableRequest(new Database(databaseName), tableName, Table.Type.LOG);
        try {
            clientAdaptor.createTable(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforCreateTable03 extends HttpConnectionImpl {
        @Override
        void doPostRequest(Request<?> request, String path, Map<String, String> header,
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
    public void testCreateDatabase03() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforCreateTable03());

        String databaseName = "testdb";
        String tableName = "testtbl";
        CreateTableRequest request = new CreateTableRequest(new Database(databaseName), tableName, Table.Type.LOG);
        try {
            clientAdaptor.createTable(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
