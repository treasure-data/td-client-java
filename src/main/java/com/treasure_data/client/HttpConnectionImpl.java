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
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
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
    private MessageDigest md = null;

    private int getReadTimeout;
    private int putReadTimeout;
    private int postReadTimeout;

    public HttpConnectionImpl() {
        this(System.getProperties());
    }

    public HttpConnectionImpl(Properties props) {
        getReadTimeout = Integer.parseInt(props.getProperty(
                Config.TD_CLIENT_GETMETHOD_READ_TIMEOUT,
                Config.TD_CLIENT_GETMETHOD_READ_TIMEOUT_DEFAULTVALUE));
        putReadTimeout = Integer.parseInt(props.getProperty(
                Config.TD_CLIENT_PUTMETHOD_READ_TIMEOUT,
                Config.TD_CLIENT_PUTMETHOD_READ_TIMEOUT_DEFAULTVALUE));
        postReadTimeout = Integer.parseInt(props.getProperty(
                Config.TD_CLIENT_POSTMETHOD_READ_TIMEOUT,
                Config.TD_CLIENT_POSTMETHOD_READ_TIMEOUT_DEFAULTVALUE));

        try {
            md = MessageDigest.getInstance("SHA-1");
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
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

        if (md != null && internalKey != null && internalKeyId != null) {
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
        sbuf.append("http://").append(getApiServerPath()).append(path);

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
        sbuf.append("http://").append(getApiServerPath()).append(path);

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
            URL url = new URL(sbuf.toString());
            conn = (HttpURLConnection) url.openConnection();
        } else {
            URL url = new URL(sbuf.toString());
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Content-Length", "0");
        }
        conn.setReadTimeout(postReadTimeout);

        // header
        conn.setRequestMethod("POST");
        setRequestAuthHeader(request, conn);
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(sbuf.toString());
        }
        conn.connect();
    }

    public void doPutRequest(Request<?> request, String path, byte[] bytes)
            throws IOException {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("http://").append(getApiServerPath()).append(path);

        URL url = new URL(sbuf.toString());
        conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(putReadTimeout);

        conn.setRequestMethod("PUT");
        //conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestProperty("Content-Length", "" + bytes.length);

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

    public void doPutRequest(Request<?> request, String path,
            InputStream in, int size) throws IOException {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("http://").append(getApiServerPath()).append(path);

        URL url = new URL(sbuf.toString());
        conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(putReadTimeout);
        conn.setRequestMethod("PUT");
        // conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestProperty("Content-Length", "" + size);

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
        conn.disconnect();
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

    private String getApiServerPath() {
        String hostAndPort = "";

        // environment variables
        hostAndPort = System.getenv(Config.TD_ENV_API_SERVER);
        if (hostAndPort != null && !hostAndPort.isEmpty()) {
            String host;
            int port;
            try {
                // parse "http://api.treasure-data.com:80/"
                URL url = new URL(hostAndPort);
                host = url.getHost();
                if (url.getPort() == -1) {
                    // parse "http://api.treasure-data.com/"
                    port = Integer.parseInt(Config.TD_API_SERVER_PORT_DEFAULTVALUE);
                } else {
                    port = url.getPort();
                }
            } catch (MalformedURLException e) {
                // no protocol
                // parse "api.treasure-data.com:80"
                String[] splited = hostAndPort.split(":");
                if (splited.length == 2) {
                    host = splited[0];
                    port = Integer.parseInt(splited[1]);
                } else {
                    // parse "api.treasure-data.com"
                    host = hostAndPort;
                    port = Integer.parseInt(Config.TD_API_SERVER_PORT_DEFAULTVALUE);
                }
            }
            return host + ":" + port;
        }

        // system properties
        Properties props = System.getProperties();
        String host = props.getProperty(
                Config.TD_API_SERVER_HOST, Config.TD_API_SERVER_HOST_DEFAULTVALUE);
        int port = Integer.parseInt(props.getProperty(
                Config.TD_API_SERVER_PORT, Config.TD_API_SERVER_PORT_DEFAULTVALUE));
        hostAndPort = host + ":" + port;

        return hostAndPort;
    }

    public static String e(String s) throws ClientException {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ClientException(e);
        }
    }

    private static String toRFC2822Format(Date from) {
        return RFC2822FORMAT.format(from);
    }
}
