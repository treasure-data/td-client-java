package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.any;
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
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.AuthenticateResult;
import com.treasure_data.model.Request;

public class TestAuthenticate {

    private DefaultClientAdaptorImpl clientAdaptor;
    private HttpConnectionImpl conn;

    @Before
    public void createResources() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        clientAdaptor = new DefaultClientAdaptorImpl(conf);

        conn = spy(new HttpConnectionImpl());
    }

    @After
    public void deleteResources() throws Exception {
        clientAdaptor = null;
        conn = null;
    }

    @Test @Ignore
    public void testAuthenticate00() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String email = props.getProperty("td.api.user");
        String password = props.getProperty("td.api.password");
        AuthenticateRequest request = new AuthenticateRequest(email, password);
        AuthenticateResult result = clientAdaptor.authenticate(request);
        System.out.println(result.getTreasureDataCredentials().getAPIKey());
    }

    @Test
    public void checkNormalBehavior() throws Exception {
        String expectedApiKey = "xxxxapikey";

        // create mock HttpConnectionImpl object
        String jsonText;
        {
            Map<String, String> map = new HashMap<String, String>();
            map.put("user", "muga");
            map.put("apikey", expectedApiKey);
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
        String email = "muga";
        String password = "nishizawa";
        AuthenticateRequest request = new AuthenticateRequest(email, password);
        AuthenticateResult result = clientAdaptor.authenticate(request);
        String gotApiKey = result.getTreasureDataCredentials().getAPIKey();
        assertEquals(expectedApiKey, gotApiKey);
    }

    @Test
    public void throwClientErrorWhenReceivedInvalidJSONAsResponseBody()
            throws Exception {
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
            String email = "muga";
            String password = "nishizawa";
            AuthenticateRequest request = new AuthenticateRequest(email, password);
            clientAdaptor.authenticate(request);
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

        // create mock HttpConnectionImpl object
        doNothing().when(conn).doPostRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(expectedCode).when(conn).getResponseCode();
        doReturn(expectedMessage).when(conn).getResponseMessage();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        try {
            String email = "muga";
            String password = "nishizawa";
            AuthenticateRequest request = new AuthenticateRequest(email, password);
            clientAdaptor.authenticate(request);
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
            String email = "muga";
            String password = "nishizawa";
            AuthenticateRequest request = new AuthenticateRequest(email, password);
            clientAdaptor.authenticate(request);
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
            String email = "muga";
            String password = "nishizawa";
            AuthenticateRequest request = new AuthenticateRequest(email, password);
            clientAdaptor.authenticate(request);
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
            String email = "muga";
            String password = "nishizawa";
            AuthenticateRequest request = new AuthenticateRequest(email, password);
            clientAdaptor.authenticate(request);
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
