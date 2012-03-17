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
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.DeleteDatabaseResult;
import com.treasure_data.model.Request;

public class TestDeleteDatabase {
    @Before
    public void setUp() throws Exception {
    }

    static class HttpConnectionImplforDeleteDatabase01 extends HttpConnectionImpl {
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
    public void testDeleteDatabase01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforDeleteDatabase01());

        String databaseName = "testdb";
        Database database = null;
        { // create
            CreateDatabaseRequest request = new CreateDatabaseRequest(databaseName);
            CreateDatabaseResult result = clientAdaptor.createDatabase(request);
            database = result.getDatabase();
            assertEquals(databaseName, database.getName());
        }

        { // delete
            DeleteDatabaseRequest request = new DeleteDatabaseRequest(database);
            DeleteDatabaseResult result = clientAdaptor.deleteDatabase(request);
            String dstName = result.getDatabaseName();
            assertEquals(databaseName, dstName);
        }
    }

    static class HttpConnectionImplforDeleteDatabase02 extends HttpConnectionImpl {
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
    public void testDeleteDatabase02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforDeleteDatabase02());

        String databaseName = "testdb";
        DeleteDatabaseRequest request = new DeleteDatabaseRequest(new Database(databaseName));
        try {
            clientAdaptor.deleteDatabase(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforDeleteDatabase03 extends HttpConnectionImpl {
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
    public void testDeleteDatabase03() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforDeleteDatabase03());

        String databaseName = "testdb";
        DeleteDatabaseRequest request = new DeleteDatabaseRequest(new Database(databaseName));
        try {
            clientAdaptor.deleteDatabase(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
