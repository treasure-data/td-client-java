//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.Unpacker;

import com.treasure_data.model.Request;

public class HttpConnectionImpl {
    private static Logger LOG = Logger.getLogger(HttpConnectionImpl.class.getName());

    private static final SimpleDateFormat RFC2822FORMAT =
        new SimpleDateFormat( "E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH );

    private HttpURLConnection conn = null;
    private Properties props;

    private int getReadTimeout;
    private int putReadTimeout;
    private int postReadTimeout;
    private Authenticator authenticator;

    public HttpConnectionImpl() {
        this(System.getProperties());
    }

    public HttpConnectionImpl(Properties props) {
        // If the given props is System.getProperties(), it changes JVM's behavior.
        getReadTimeout = Integer.parseInt(props.getProperty(
                Config.TD_CLIENT_GETMETHOD_READ_TIMEOUT,
                Config.TD_CLIENT_GETMETHOD_READ_TIMEOUT_DEFAULTVALUE));
        putReadTimeout = Integer.parseInt(props.getProperty(
                Config.TD_CLIENT_PUTMETHOD_READ_TIMEOUT,
                Config.TD_CLIENT_PUTMETHOD_READ_TIMEOUT_DEFAULTVALUE));
        postReadTimeout = Integer.parseInt(props.getProperty(
                Config.TD_CLIENT_POSTMETHOD_READ_TIMEOUT,
                Config.TD_CLIENT_POSTMETHOD_READ_TIMEOUT_DEFAULTVALUE));
        String username = props.getProperty(Config.HTTP_PROXY_USER);
        String password = props.getProperty(Config.HTTP_PROXY_PASSWORD);
        if (username != null && password != null) {
            setDefaultAuthenticator(username, password.toCharArray());
        }
        this.props = props;
    }

    // Bear in mind that this method changes HttpURLConnection's WWW-Authorization / Proxy-Authorization behavior in a JVM process.
    private void setDefaultAuthenticator(final String username, final char[] password) {
        authenticator = new Authenticator() {
            public PasswordAuthentication getPasswordAuthentication() {
                return (new PasswordAuthentication(username, password));
            }
        };
        Authenticator.setDefault(authenticator);
        if (LOG.isLoggable(Level.INFO)) {
            LOG.info("Set authentication username: " + username);
        }
    }

    Authenticator getAuthenticator() {
        return authenticator;
    }

    private void setRequestAuthHeader(Request<?> request, HttpURLConnection conn) throws IOException {
        String apiKey = request.getCredentials().getAPIKey();
        if (apiKey != null) {
            conn.setRequestProperty("Authorization", "TD1 " + apiKey);
        }
        String internalKey = request.getCredentials().getInternalKey();
        String internalKeyId = request.getCredentials().getInternalKeyId();
        String dateStr = toRFC2822Format(new Date());
        conn.setRequestProperty("Date", dateStr);

        if (internalKey != null && internalKeyId != null) {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SHA-1 digest algorithm must be available but not found", e);
            }
            md.reset();
            md.update((String.format("%s\n%s\n", dateStr, internalKey)).getBytes());
            String hashedKey = byteArrayToHexString(md.digest());
            conn.setRequestProperty("Internal-Authorization", String.format("TD2 %s:%s", internalKeyId, hashedKey));
        }
    }

    // http://rgagnon.com/javadetails/java-0596.html
    private static String byteArrayToHexString(byte[] b) {
        String result = "";
        for (int i=0; i < b.length; i++) {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }
        return result;
    }

    public void doGetRequest(Request<?> request, String path, Map<String, String> header,
            Map<String, String> params) throws IOException {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(getSchemeHostPort(System.getenv(Config.TD_ENV_API_SERVER))).append(path);

        // parameters
        if (params != null && !params.isEmpty()) {
            sbuf.append("?");
            int paramSize = params.size();
            Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
            for (int i = 0; i < paramSize; ++i) {
                Map.Entry<String, String> e = iter.next();
                sbuf.append(e.getKey()).append("=").append(e.getValue());
                if (i + 1 != paramSize) {
                    sbuf.append("&");
                }
            }
        }

        // create connection object with url
        URL url = new URL(sbuf.toString());
        conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(getReadTimeout);

        // header
        conn.setRequestMethod("GET");
        setRequestAuthHeader(request, conn);
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }

        // do connection to server
        conn.connect();
    }

    public void doPostRequest(Request<?> request, String path, Map<String, String> header,
            Map<String, String> params) throws IOException {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(getSchemeHostPort(System.getenv(Config.TD_ENV_API_SERVER))).append(path);

        URL url = new URL(sbuf.toString());
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setReadTimeout(postReadTimeout);

        // header
        setRequestAuthHeader(request, conn);
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }

        // parameters
        if (params != null && !params.isEmpty()) {
            StringBuilder queryParam = new StringBuilder();
            int paramSize = params.size();
            Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
            for (int i = 0; i < paramSize; ++i) {
                Map.Entry<String, String> e = iter.next();
                queryParam.append(e.getKey()).append("=").append(e.getValue());
                if (i + 1 != paramSize) {
                    queryParam.append("&");
                }
            }

            byte[] bodyData = queryParam.toString().getBytes("UTF-8");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setRequestProperty("Content-Length", Integer.toString(bodyData.length));
            conn.setDoOutput(true);
            OutputStream body = conn.getOutputStream();
            body.write(bodyData);
            body.flush();
        } else {
            conn.setRequestProperty("Content-Length", "0");
        }

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(sbuf.toString());
        }
        conn.connect();
    }

    public void doPutRequest(
            Request<?> request,
            String path,
            Map<String, String> header,
            byte[] bytes)
                    throws IOException {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(getSchemeHostPort(System.getenv(Config.TD_ENV_API_SERVER))).append(path);

        URL url = new URL(sbuf.toString());
        conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(putReadTimeout);

        conn.setRequestMethod("PUT");
        //conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestProperty("Content-Length", "" + bytes.length);
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }

        setRequestAuthHeader(request, conn);
        conn.setDoOutput(true);
        conn.setUseCaches (false);
        //conn.connect();

        // body
        BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
        out.write(bytes);
        out.flush();
        //out.close();
    }

    public void doPutRequest(
            Request<?> request,
            String path,
            Map<String, String> header,
            InputStream in,
            int size) throws IOException {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(getSchemeHostPort(System.getenv(Config.TD_ENV_API_SERVER))).append(path);

        URL url = new URL(sbuf.toString());
        conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(putReadTimeout);
        conn.setRequestMethod("PUT");
        // conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestProperty("Content-Length", "" + size);
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }

        setRequestAuthHeader(request, conn);
        conn.setDoOutput(true);
        conn.setUseCaches(false);
        // conn.connect();

        // body
        BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
        byte[] buf = new byte[1024];
        int len;
//        int count = 0;
//        int flushThreshold = 128;
        while ((len = in.read(buf)) != -1) {
            out.write(buf, 0, len);
//            count++;
//            if (count > flushThreshold) {
//                out.flush();
//            }
        }
        out.flush();
        // out.close();
    }

    public int getResponseCode() throws IOException {
        return conn.getResponseCode();
    }

    public String getResponseMessage() throws IOException {
        return conn.getResponseMessage();
    }

    public String getResponseBody() throws IOException {
        StringBuilder sbuf = new StringBuilder();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream()));
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            sbuf.append(line);
        }
        reader.close();
        return sbuf.toString();
    }

    public String getErrorMessage() throws IOException {
        StringBuilder sbuf = new StringBuilder();

        InputStream orig = conn.getErrorStream();
        if (orig == null) {
            return "No error message";
        }

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(orig));
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            sbuf.append(line);
        }
        reader.close();
        return sbuf.toString();
    }

    public void disconnect() {
        if (conn != null) {
            conn.disconnect();
        }
    }

    public int getContentLength() throws IOException {
        return conn.getContentLength();
    }

    public Unpacker getResponseBodyBinary() throws IOException {
        BufferedInputStream in = new BufferedInputStream(getInputStream());
        MessagePack msgpack = new MessagePack();
        BufferUnpacker unpacker = msgpack.createBufferUnpacker();
        byte[] buf = new byte[1024];

        int len = 0;
        while ((len = in.read(buf)) != -1) {
            unpacker.feed(buf, 0, len);
        }

        return unpacker;
    }

    public Unpacker getResponseBodyBinaryWithGZip() throws IOException {
        InputStream in = new GZIPInputStream(getInputStream());
        MessagePack msgpack = new MessagePack();
        BufferUnpacker unpacker = msgpack.createBufferUnpacker();
        byte[] buf = new byte[1024];

        int len = 0;
        while ((len = in.read(buf)) != -1) {
            unpacker.feed(buf, 0, len);
        }

        return unpacker;
    }

    public Unpacker getResponseBodyBinaryWithGZip2() throws IOException {
        MessagePack msgpack = new MessagePack();
        InputStream in = new BufferedInputStream(new GZIPInputStream(getInputStream()));
        return msgpack.createUnpacker(in);
    }

    public InputStream getInputStream() throws IOException {
        return conn.getInputStream();
    }

    String getSchemeHostPort(String urlString) {
        String scheme, host;
        int port;

        if (urlString == null || urlString.isEmpty()) {
            scheme = props.getProperty(
                    Config.TD_CK_API_SERVER_SCHEME, Config.TD_API_SERVER_SCHEME_DEFAULTVALUE);
            host = props.getProperty(
                    Config.TD_API_SERVER_HOST, Config.TD_API_SERVER_HOST_DEFAULTVALUE);
            port = Integer.parseInt(props.getProperty(
                    Config.TD_API_SERVER_PORT, Config.TD_API_SERVER_PORT_DEFAULTVALUE));
            return scheme + host + ":" + port;
        }

        try {
            // parse "http://api.treasure-data.com:80/"
            URL url = new URL(urlString);
            scheme = url.getProtocol() + "://";
            host = url.getHost();
            if (url.getPort() == -1) {
                // parse "http://api.treasure-data.com/"
                String p;
                if (scheme.equals(Config.TD_API_SERVER_SCHEME_HTTPS)) {
                    p = Config.TD_API_SERVER_PORT_DEFAULTVALUE;
                } else {
                    p = Config.TD_API_SERVER_PORT_HTTP;
                }
                port = Integer.parseInt(p);
            } else {
                port = url.getPort();
            }
        } catch (MalformedURLException e) {
            // no scheme

            if (urlString.lastIndexOf('/') == urlString.length() - 1) {
                urlString = urlString.substring(0, urlString.length() - 1);
            }

            // parse "api.treasure-data.com:80"
            String[] splited = urlString.split(":");
            if (splited.length == 2) {
                host = splited[0];
                port = Integer.parseInt(splited[1]);
                if (443 == port) {
                    scheme = Config.TD_API_SERVER_SCHEME_DEFAULTVALUE;
                } else {
                    scheme = Config.TD_API_SERVER_SCHEME_HTTP;
                }
            } else {
                // parse "api.treasure-data.com"
                host = urlString;
                scheme = Config.TD_API_SERVER_SCHEME_DEFAULTVALUE;
                port = Integer.parseInt(Config.TD_API_SERVER_PORT_DEFAULTVALUE);
            }
        }

        return scheme + host + ":" + port;
    }

    public static String e(String s) throws ClientException {
        try {
            return URLEncoder.encode(s, "UTF-8").replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new ClientException(e);
        }
    }

    private static String toRFC2822Format(Date from) {
        return RFC2822FORMAT.format(from);
    }
}
