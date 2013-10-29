package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.Request;
import com.treasure_data.model.Result;

@Ignore
public abstract class AnyMethodTestUtil<REQ extends Request<?>, RET extends Result<?>, CLIENT extends AbstractClientAdaptor> {

    protected CLIENT clientAdaptor;
    protected HttpConnectionImpl conn;
    protected boolean responsedBinary = false;

    @Before
    public void createResources() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        clientAdaptor = createClientAdaptorImpl(conf);

        conn = spy(new HttpConnectionImpl());
    }

    protected CLIENT createClientAdaptorImpl(Config conf) {
        throw new UnsupportedOperationException();
    }

    @After
    public void deleteResources() throws Exception {
        clientAdaptor = null;
        conn = null;
    }

    protected RET doBusinessLogic() throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void checkNormalBehavior0() throws Exception {
        throw new UnsupportedOperationException();
    }

    protected String getJSONTextForChecking() {
        throw new UnsupportedOperationException();
    }

    protected Unpacker getMockResponseBodyBinary() {
        throw new UnsupportedOperationException();
    }

    protected abstract void callMockDoMethodRequest() throws Exception;

    @Test
    public void checkNormalBehavior() throws Exception {
        // create mock HttpConnectionImpl object
        callMockDoMethodRequest();
        doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn("something").when(conn).getErrorMessage();
        if (!responsedBinary) {
            doReturn(getJSONTextForChecking()).when(conn).getResponseBody();
        } else {
            doReturn(getMockResponseBodyBinary()).when(conn).getResponseBodyBinary();
        }
        doNothing().when(conn).disconnect();
        clientAdaptor.setConnection(conn);

        // check behavior
        checkNormalBehavior0();
    }

    protected void throwClientErrorWhenReceivedInvalidJSONAsResponseBody0()
            throws Exception {
        doBusinessLogic();
    }

    @Test
    public void throwClientErrorWhenReceivedInvalidJSONAsResponseBody()
            throws Exception {
        // create mock HttpConnectionImpl object
        callMockDoMethodRequest();
        doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn("something").when(conn).getErrorMessage();
        if (!responsedBinary) {
            doReturn("invalid_json").when(conn).getResponseBody();
        } else {
            InputStream in = new ByteArrayInputStream(new byte[] { 0x01 });
            Unpacker unpacker = new MessagePack().createUnpacker(in);
            doReturn(unpacker).when(conn).getResponseBodyBinary();
        }
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

    @Test
    public void throwClientErrorWhenReceivedNonOKResponseCode()
            throws Exception {
        int expectedCode = HttpURLConnection.HTTP_BAD_REQUEST;
        String expectedMessage = "something";
        String expectedErrMessage = "something2";

        // create mock HttpConnectionImpl object
        callMockDoMethodRequest();
        doReturn(expectedCode).when(conn).getResponseCode();
        doReturn(expectedMessage).when(conn).getResponseMessage();
        doReturn(expectedErrMessage).when(conn).getErrorMessage();
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
            assertEquals(expectedMessage + ", detail = " + expectedErrMessage, e.getResponseMessage());
        }
    }

    protected void throwClientErrorWhenGetResponseCodeThrowsIOError0()
            throws Exception {
        doBusinessLogic();
    }

    @Test
    public void throwClientErrorWhenGetResponseCodeThrowsIOError()
            throws Exception {
        // create mock HttpConnectionImpl object
        callMockDoMethodRequest();
        doThrow(new IOException()).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn("something").when(conn).getErrorMessage();
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

    @Test
    public void throwClientErrorWhenGetResponseMessageThrowsIOError()
            throws Exception {
        int expectedCode = HttpURLConnection.HTTP_BAD_REQUEST;

        // create mock HttpConnectionImpl object
        callMockDoMethodRequest();
        doReturn(expectedCode).when(conn).getResponseCode();
        doThrow(new IOException()).when(conn).getResponseMessage();
        doReturn("something").when(conn).getErrorMessage();
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

    @Test
    public void throwClientErrorWhenGetResponseBodyThrowsIOError()
            throws Exception {
        int expectedCode = HttpURLConnection.HTTP_OK;
        String expectedMessage = "something";

        // create mock HttpConnectionImpl object
        callMockDoMethodRequest();
        doReturn(expectedCode).when(conn).getResponseCode();
        doReturn("something").when(conn).getResponseMessage();
        doReturn("something").when(conn).getErrorMessage();
        if (!responsedBinary) {
            doThrow(new IOException()).when(conn).getResponseBody();
        } else {
            doReturn(new IOException()).when(conn).getResponseBodyBinary();
        }
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
