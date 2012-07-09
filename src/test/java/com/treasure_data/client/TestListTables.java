package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpConnectionImpl;
import com.treasure_data.model.Database;
import com.treasure_data.model.ListTablesRequest;
import com.treasure_data.model.ListTablesResult;
import com.treasure_data.model.Request;
import com.treasure_data.model.TableSummary;

public class TestListTables {
    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void testListTables00() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        //ListTablesRequest request = new ListTablesRequest(new Database("test_merge_0"));
        ListTablesRequest request = new ListTablesRequest(new Database("mugatest"));
        ListTablesResult result = clientAdaptor.listTables(request);
        List<TableSummary> tables = result.getTables();
        for (TableSummary table : tables) {
            System.out.println(table.getDatabase().getName());
            System.out.println(table.getName());
            System.out.println(table.getSchema());
            System.out.println(table.getType());
            System.out.println(table.getCount());
        }
    }

    static class HttpConnectionImplforListTables01 extends HttpConnectionImpl {
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
            map.put("database", "testdb");
            List tbls = new ArrayList();
            Map m0 = new HashMap();
            m0.put("type", "item");
            m0.put("name", "foo");
            m0.put("count", 13123233);
            tbls.add(m0);
            Map m1 = new HashMap();
            m1.put("type", "item");
            m1.put("name", "bar");
            m1.put("count", 331232);
            tbls.add(m1);
            map.put("tables", tbls);
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
    public void testListTables01() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListTables01());

        String databaseName = "testdb";
        ListTablesRequest request = new ListTablesRequest(new Database(databaseName));
        ListTablesResult result = clientAdaptor.listTables(request);
        List<TableSummary> tables = result.getTables();
        assertEquals(2, tables.size());
        assertEquals("foo", tables.get(0).getName());
        assertEquals("bar", tables.get(1).getName());
    }

    static class HttpConnectionImplforListTables02 extends HttpConnectionImpl {
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
    public void testListTables02() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListTables02());

        String databaseName = "testdb";
        ListTablesRequest request = new ListTablesRequest(new Database(databaseName));
        try {
            clientAdaptor.listTables(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforListTables03 extends HttpConnectionImpl {
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
    public void testListTables03() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListTables03());

        String databaseName = "testdb";
        ListTablesRequest request = new ListTablesRequest(new Database(databaseName));
        try {
            clientAdaptor.listTables(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
