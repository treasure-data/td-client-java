package com.treasure_data.client;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class TestHttpConnectionImpl {

    protected Properties props;

    @Before
    public void createResources() throws Exception {
        props = new Properties();
    }

    @Test
    public void defaultURL() throws Exception {
        assertURL(props, null,
                "https://api.treasuredata.com:443");
    }

    @Test
    public void customUrlWith01() throws Exception {
        assertURL(props, "http://api.treasure-data.com:8080",
                "http://api.treasure-data.com:8080");
    }

    @Test
    public void customUrlWith02() throws Exception {
        assertURL(props, "http://api.treasure-data.com:8080/",
                "http://api.treasure-data.com:8080");
    }

    @Test
    public void customUrlWith03() throws Exception {
        assertURL(props, "api.treasure-data.com:8080",
                "http://api.treasure-data.com:8080");
    }

    @Test
    public void customUrlWith04() throws Exception {
        assertURL(props, "api.treasure-data.com:8080/",
                "http://api.treasure-data.com:8080");
    }

    @Test
    public void urlWithHttps01() throws Exception {
        assertURL(props, "https://api.treasure-data.com:443",
                "https://api.treasure-data.com:443");
    }

    @Test
    public void urlWithHttps02() throws Exception {
        assertURL(props, "https://api.treasure-data.com:443/",
                "https://api.treasure-data.com:443");
    }

    @Test
    public void urlWithoutHttpsSchemeAndPort() throws Exception {
        assertURL(props, "api.treasure-data.com",
                "https://api.treasure-data.com:443");
    }

    @Test
    public void urlWithoutHttpsScheme01() throws Exception {
        assertURL(props, "api.treasure-data.com:443",
                "https://api.treasure-data.com:443");
    }

    @Test
    public void urlWithoutHttpsScheme02() throws Exception {
        assertURL(props, "api.treasure-data.com:443/",
                "https://api.treasure-data.com:443");
    }

    @Test
    public void urlWithoutHttpsPort01() throws Exception {
        assertURL(props, "https://api.treasure-data.com",
                "https://api.treasure-data.com:443");
    }

    @Test
    public void urlWithoutHttpsPort02() throws Exception {
        assertURL(props, "https://api.treasure-data.com/",
                "https://api.treasure-data.com:443");
    }

    @Test
    public void urlWithHttp01() throws Exception {
        assertURL(props, "http://api.treasure-data.com:80",
                "http://api.treasure-data.com:80");
    }

    @Test
    public void urlWithHttp02() throws Exception {
        assertURL(props, "http://api.treasure-data.com:80/",
                "http://api.treasure-data.com:80");
    }

    @Test
    public void urlWithoutHttpScheme01() throws Exception {
        assertURL(props, "api.treasure-data.com:80",
                "http://api.treasure-data.com:80");
    }

    @Test
    public void urlWithoutHttpScheme02() throws Exception {
        assertURL(props, "api.treasure-data.com:80/",
                "http://api.treasure-data.com:80");
    }

    @Test
    public void urlWithoutHttpPort01() throws Exception {
        assertURL(props, "http://api.treasure-data.com",
                "http://api.treasure-data.com:80");
    }

    @Test
    public void urlWithoutHttpPort02() throws Exception {
        assertURL(props, "http://api.treasure-data.com/",
                "http://api.treasure-data.com:80");
    }

    public static void assertURL(Properties props, String urlString, String expected) throws Exception {
        HttpConnectionImpl conn = new HttpConnectionImpl(props);

        String actual = conn.getSchemeHostPort(urlString);
        assertEquals(expected, actual);
    }
}
