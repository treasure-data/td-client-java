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
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test @Ignore
    public void testAuthenticate00() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);

        String email = props.getProperty("td.api.user");
        String password = props.getProperty("td.api.password");
        AuthenticateRequest request = new AuthenticateRequest(email, password);
        AuthenticateResult result = clientAdaptor.authenticate(request);
        System.out.println(result.getTreasureDataCredentials().getAPIKey());
    }

    static class HttpConnectionImplforAuthenticate01 extends HttpConnectionImpl {
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
            map.put("user", "muga");
            map.put("apikey", "nishizawa");
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
    public void testAuthenticate01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforAuthenticate01());

        String email = "muga";
        String password = "nishizawa";
        AuthenticateRequest request = new AuthenticateRequest(email, password);
        AuthenticateResult result = clientAdaptor.authenticate(request);
        assertEquals("nishizawa", result.getTreasureDataCredentials().getAPIKey());
    }

    static class HttpConnectionImplforAuthenticate02 extends HttpConnectionImpl {
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
    public void testAuthenticate02() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforAuthenticate02());

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

    static class HttpConnectionImplforAuthenticate03 extends HttpConnectionImpl {
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
    public void testAuthenticate03() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        HttpClientAdaptor clientAdaptor = new HttpClientAdaptor(conf);
        clientAdaptor.setConnection(new HttpConnectionImplforAuthenticate03());

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
}
