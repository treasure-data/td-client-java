package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.HttpConnectionImpl;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.ListDatabasesRequest;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.Request;

public class TestListDatabases {

    private DefaultClientAdaptorImpl clientAdaptor;
    private HttpConnectionImpl conn;
    private ListDatabasesRequest request;

    @Before
    public void createResources() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        clientAdaptor = new DefaultClientAdaptorImpl(conf);

        conn = spy(new HttpConnectionImpl());

        request = new ListDatabasesRequest();
    }

    @After
    public void deleteResources() throws Exception {
        clientAdaptor = null;
        conn = null;
        request = null;
    }

    @Test @Ignore
    public void testListDatabases00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        ListDatabasesRequest request = new ListDatabasesRequest();
        ListDatabasesResult result = clientAdaptor.listDatabases(request);
        List<DatabaseSummary> databases = result.getDatabases();
        for (DatabaseSummary database : databases) {
            System.out.println(database.getName());
        }
    }

    @Test
    public void checkNormalBehavior() throws Exception {
        // create mock HttpConnectionImpl class
        String jsonText;
        {
            List ary = new ArrayList();
            Map m0 = new HashMap();
            m0.put("name", "foo");
            m0.put("count", 10);
            m0.put("created_at", "created_time");
            m0.put("updated_at", "updated_time");
            ary.add(m0);
            Map m1 = new HashMap();
            m1.put("name", "bar");
            m1.put("count", 10);
            m1.put("created_at", "created_time");
            m1.put("updated_at", "updated_time");
            ary.add(m1);
            Map map = new HashMap();
            map.put("databases", ary);
            jsonText = JSONValue.toJSONString(map);
        }
        doNothing().when(conn).doGetRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn(jsonText).when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        ListDatabasesResult result = clientAdaptor.listDatabases(request);
        List<DatabaseSummary> databases = result.getDatabases();
        assertEquals(2, databases.size());
        assertEquals("foo", databases.get(0).getName());
        assertEquals("bar", databases.get(1).getName());
    }

    @Test
    public void throwClientErrorWhenReceivedInvalidJSONAsResponseBody()
            throws Exception {
        // create mock HttpConnectionImpl class
        doNothing().when(conn).doGetRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn("invalid_json").when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.listDatabases(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    @Test
    public void throwClientErrorWhenReceivedNonOKResponseCode()
            throws Exception {
        int expectedCode = HttpURLConnection.HTTP_BAD_REQUEST;
        String expectedMessage = "something";

        // create mock HttpConnectionImpl class
        doNothing().when(conn).doGetRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(expectedCode).when(conn).getResponseCode();
        doReturn(expectedMessage).when(conn).getResponseMessage();
        doReturn("something").when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        try {
            clientAdaptor.listDatabases(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof HttpClientException);
            HttpClientException e = (HttpClientException) t;
            assertEquals(expectedCode, e.getResponseCode());
            assertEquals(expectedMessage, e.getResponseMessage());
        }
    }

    @Test
    public void throwClientErrorWhenGetResponseCodeThrowsIOError()
            throws Exception {
        // create mock HttpConnectionImpl class
        doNothing().when(conn).doGetRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doThrow(new IOException()).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn("something").when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.listDatabases(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof HttpClientException);
            HttpClientException e = (HttpClientException) t;
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void throwClientErrorWhenGetResponseMessageThrowsIOError()
            throws Exception {
        int expectedCode = HttpURLConnection.HTTP_BAD_REQUEST;

        // create mock HttpConnectionImpl class
        doNothing().when(conn).doGetRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(expectedCode).when(conn).getResponseCode();
        doThrow(new IOException()).when(conn).getResponseMessage();
        doReturn("something").when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.listDatabases(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof HttpClientException);
            HttpClientException e = (HttpClientException) t;
            assertTrue(e.getCause() instanceof IOException);
            assertEquals(expectedCode, e.getResponseCode());
        }
    }

    @Test
    public void throwClientErrorWhenGetResponseBodyThrowsIOError()
            throws Exception {
        int expectedCode = HttpURLConnection.HTTP_OK;
        String expectedMessage = "something";

        // create mock HttpConnectionImpl class
        doNothing().when(conn).doGetRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(expectedCode).when(conn).getResponseCode();
        doReturn(expectedMessage).when(conn).getResponseMessage();
        doThrow(new IOException()).when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.listDatabases(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof HttpClientException);
            HttpClientException e = (HttpClientException) t;
            assertTrue(e.getCause() instanceof IOException);
            assertEquals(expectedCode, e.getResponseCode());
            assertEquals(expectedMessage, e.getResponseMessage());
        }
    }
}
