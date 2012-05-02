package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.json.simple.JSONValue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpClientAdaptor.HttpConnectionImpl;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.ImportRequest;
import com.treasure_data.model.ImportResult;
import com.treasure_data.model.Request;
import com.treasure_data.model.Table;

public class TestImportData {
    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void testImportData00() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);

        String databaseName = "mugadb";
        String tableName = "testtbl";
        Table dstTable = null;
        try {
            CreateTableRequest preReq = new CreateTableRequest(
                    new Database(databaseName), tableName);
            CreateTableResult preRes = clientAdaptor.createTable(preReq);
            Table srcTable = preRes.getTable();

            byte[] data = createData();
            ImportRequest request = new ImportRequest(srcTable, data);
            ImportResult result = clientAdaptor.importData(request);
            dstTable = result.getTable();
            double dstTime = result.getElapsedTime();
            System.out.println("dst table: " + dstTable);
            System.out.println("dst time: " + dstTime);
        } finally {
            if (dstTable != null) {
                // delete table
                clientAdaptor.deleteTable(new DeleteTableRequest(dstTable));
            }
        }
    }

    private byte[] createData() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(out);
        MessagePack msgpack = new MessagePack();
        Packer packer = msgpack.createPacker(gzout);

        for (int i = 0; i < 100; i++) {
            long time = System.currentTimeMillis() / 1000;
            Map<String, Object> record = new HashMap<String, Object>();
            record.put("name", "muga:" + i);
            record.put("id", i);
            record.put("time", time);
            packer.write(record);
        }

        gzout.finish();
        return out.toByteArray();
    }

    static class HttpConnectionImplforImportData01 extends HttpConnectionImpl {
        @Override
        HttpURLConnection doPutRequest(Request<?> request, String path, byte[] bytes)
                throws IOException {
            // do nothing
            return null;
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
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("database", "testdb");
            map.put("table", "testtbl");
            map.put("elapsed_time", 32.3);
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
    public void testImportData01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforImportData01());

        String databaseName = "testdb";
        String tableName = "testtbl";
        Table table = new Table(new Database(databaseName), tableName);
        byte[] data = new byte[0];
        ImportRequest request = new ImportRequest(table, data);
        ImportResult result = clientAdaptor.importData(request);
        assertEquals(databaseName, result.getTable().getDatabase().getName());
        assertEquals(tableName, result.getTable().getName());
    }

    static class HttpConnectionImplforImportData02 extends HttpConnectionImpl {
        @Override
        HttpURLConnection doPutRequest(Request<?> request, String path, byte[] bytes)
                throws IOException {
            // do nothing
            return null;
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
    public void testImportData02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforImportData02());

        try {
            String databaseName = "testdb";
            String tableName = "testtbl";
            Table table = new Table(new Database(databaseName), tableName);
            byte[] data = new byte[0];
            ImportRequest request = new ImportRequest(table, data);
            clientAdaptor.importData(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforImportData03 extends HttpConnectionImpl {
        @Override
        HttpURLConnection doPutRequest(Request<?> request, String path, byte[] bytes)
                throws IOException {
            // do nothing
            return null;
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
    public void testImportData03() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforImportData03());

        try {
            String databaseName = "testdb";
            String tableName = "testtbl";
            Table table = new Table(new Database(databaseName), tableName);
            byte[] data = new byte[0];
            ImportRequest request = new ImportRequest(table, data);
            clientAdaptor.importData(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
