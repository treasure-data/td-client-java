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
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.Request;

@Ignore
public class GetMethodTestUtil {

    protected DefaultClientAdaptorImpl clientAdaptor;
    protected HttpConnectionImpl conn;

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

    protected void doBusinessLogic() throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void checkNormalBehavior0() throws Exception {
        throw new UnsupportedOperationException();
    }

    protected String getJSONTextForChecking() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void checkNormalBehavior() throws Exception {
        // create mock HttpConnectionImpl object
        String jsonText = getJSONTextForChecking();
        doNothing().when(conn).doGetRequest(any(Request.class),
                any(String.class), any(Map.class), any(Map.class));
        doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn(jsonText).when(conn).getResponseBody();
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        checkNormalBehavior0();
    }

    protected void throwClientErrorWhenReceivedInvalidJSONAsResponseBody0()
            throws Exception {
        doBusinessLogic();
    }

    @SuppressWarnings("unchecked")
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
            throwClientErrorWhenReceivedInvalidJSONAsResponseBody0();
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof ClientException);
        }
    }

    protected void throwClientErrorWhenReceivedNonOKResponseCode0()
            throws Exception {
        doBusinessLogic();
    }

    @SuppressWarnings("unchecked")
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
            throwClientErrorWhenReceivedNonOKResponseCode0();
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof HttpClientException);
            HttpClientException e = (HttpClientException) t;
            assertEquals(expectedCode, e.getResponseCode());
            assertEquals(expectedMessage, e.getResponseMessage());
        }
    }

    protected void throwClientErrorWhenGetResponseCodeThrowsIOError0()
            throws Exception {
        doBusinessLogic();
    }

    @SuppressWarnings("unchecked")
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
            throwClientErrorWhenGetResponseCodeThrowsIOError0();
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof HttpClientException);
            HttpClientException e = (HttpClientException) t;
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    protected void throwClientErrorWhenGetResponseMessageThrowsIOError0()
            throws Exception {
        doBusinessLogic();
    }

    @SuppressWarnings("unchecked")
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
            throwClientErrorWhenGetResponseMessageThrowsIOError0();
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof HttpClientException);
            HttpClientException e = (HttpClientException) t;
            assertTrue(e.getCause() instanceof IOException);
            assertEquals(expectedCode, e.getResponseCode());
        }
    }

    protected void throwClientErrorWhenGetResponseBodyThrowsIOError0()
            throws Exception {
        doBusinessLogic();
    }

    @SuppressWarnings("unchecked")
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
            throwClientErrorWhenGetResponseBodyThrowsIOError0();
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
