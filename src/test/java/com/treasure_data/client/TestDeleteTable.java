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
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpConnectionImpl;
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.DeleteTableResult;
import com.treasure_data.model.Request;
import com.treasure_data.model.Table;

public class TestDeleteTable {

    @Test @Ignore
    public void testDeleteTable00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "db1";
        try {
            // create database
            CreateDatabaseResult ret =
                clientAdaptor.createDatabase(new CreateDatabaseRequest(databaseName));
            Database database = ret.getDatabase();
            CreateTableRequest req = new CreateTableRequest(database, "test01");
            CreateTableResult res = clientAdaptor.createTable(req);
            Table table = res.getTable();

            DeleteTableRequest request = new DeleteTableRequest(table);
            DeleteTableResult result = clientAdaptor.deleteTable(request);
            System.out.println(result.getTableName());
        } finally {
            // delete database
            clientAdaptor.deleteDatabase(new DeleteDatabaseRequest(databaseName));
        }
    }

    static class HttpConnectionImplforDeleteTable01 extends HttpConnectionImpl {
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
            map.put("table", "testtbl");
            map.put("type", "log");
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
    public void testDeleteTable01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforDeleteTable01());

        String databaseName = "testdb";
        String tableName = "testtbl";
        DeleteTableRequest request = new DeleteTableRequest(new Table(
                new Database(databaseName), tableName, Table.Type.LOG));
        DeleteTableResult result = clientAdaptor.deleteTable(request);
        assertEquals(databaseName, result.getDatabase().getName());
        assertEquals(tableName, result.getTableName());
    }

    static class HttpConnectionImplforDeleteTable02 extends HttpConnectionImpl {
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
    public void testDeleteTable02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforDeleteTable02());

        String databaseName = "testdb";
        String tableName = "testtble";
        DeleteTableRequest request = new DeleteTableRequest(new Table(
                new Database(databaseName), tableName, Table.Type.LOG));
        try {
            clientAdaptor.deleteTable(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforDeleteTable03 extends HttpConnectionImpl {
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
    public void testDeleteTable03() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforDeleteTable03());

        String databaseName = "testdb";
        String tableName = "testtble";
        DeleteTableRequest request = new DeleteTableRequest(new Table(
                new Database(databaseName), tableName, Table.Type.LOG));
        try {
            clientAdaptor.deleteTable(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
