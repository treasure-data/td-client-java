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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.After;
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

    private DefaultClientAdaptorImpl clientAdaptor;
    private HttpConnectionImpl conn;
    private String databaseName;
    private CreateDatabaseRequest request;

    @Before
    public void createResources() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        clientAdaptor = new DefaultClientAdaptorImpl(conf);

        conn = spy(new HttpConnectionImpl());

        databaseName = "testdb";
        request = new CreateDatabaseRequest(databaseName);
    }

    @After
    public void deleteResources() throws Exception {
        clientAdaptor = null;
        conn = null;
        databaseName = null;
        request = null;
    }

    @Test @Ignore
    public void testCreateDatabase00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

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

    @Test
    public void checkNormalBehavior() throws Exception {
        // create mock HttpConnectionImpl object
        String jsonText;
        {
            Map<String, String> map = new HashMap<String, String>();
            map.put("database", databaseName);
            jsonText = JSONValue.toJSONString(map);
        }
        doNothing().when(conn).doPostRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn(jsonText).when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        CreateDatabaseResult result = clientAdaptor.createDatabase(request);
        assertEquals(databaseName, result.getDatabase().getName());
    }

    @Test
    public void throwClientErrorWhenReceivedInvalidJSONAsResponseBody() throws Exception {
        // create mock HttpConnectionImpl object
        doNothing().when(conn).doPostRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn("invalid_json").when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.createDatabase(request);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    @Test
    public void throwClientErrorWhenReceivedNonOKResponseCode() throws Exception {
        int expectedCode = HttpURLConnection.HTTP_BAD_REQUEST;
        String expectedMessage = "something";

        // create mock HttpConnectionImpl object
        doNothing().when(conn).doPostRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(expectedCode).when(conn).getResponseCode();
        doReturn(expectedMessage).when(conn).getResponseMessage();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.createDatabase(request);
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
        // create mock HttpConnectionImpl object
        doNothing().when(conn).doPostRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doThrow(new IOException()).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.createDatabase(request);
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

        // create mock HttpConnectionImpl object
        doNothing().when(conn).doPostRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(expectedCode).when(conn).getResponseCode();
        doThrow(new IOException()).when(conn).getResponseMessage();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.createDatabase(request);
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

        // create mock HttpConnectionImpl object
        doNothing().when(conn).doPostRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(expectedCode).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doThrow(new IOException()).when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            clientAdaptor.createDatabase(request);
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
