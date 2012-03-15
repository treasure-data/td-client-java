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
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpClientAdaptor.HttpConnectionImpl;
import com.treasure_data.model.Database;
import com.treasure_data.model.ListDatabasesRequest;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.Request;

public class TestListDatabases {
    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    static class HttpConnectionImplforListDatabases01 extends HttpConnectionImpl {
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
            List ary = new ArrayList();
            Map m0 = new HashMap();
            m0.put("name", "foo");
            ary.add(m0);
            Map m1 = new HashMap();
            m1.put("name", "bar");
            ary.add(m1);
            Map map = new HashMap();
            map.put("databases", ary);
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
    public void testListDatabases01() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListDatabases01());

        ListDatabasesRequest request = new ListDatabasesRequest();
        ListDatabasesResult result = clientAdaptor.listDatabases(request);
        List<Database> databases = result.getDatabases();
        assertEquals(2, databases.size());
        assertEquals("foo", databases.get(0).getName());
        assertEquals("bar", databases.get(1).getName());
    }

    static class HttpConnectionImplforListDatabases02 extends HttpConnectionImpl {
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
    public void testListDatabases02() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListDatabases02());
        ListDatabasesRequest request = new ListDatabasesRequest();
        try {
            clientAdaptor.listDatabases(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    static class HttpConnectionImplforListDatabases03 extends HttpConnectionImpl {
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
    public void testListDatabases03() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforListDatabases03());
        ListDatabasesRequest request = new ListDatabasesRequest();
        try {
            clientAdaptor.listDatabases(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }
}
