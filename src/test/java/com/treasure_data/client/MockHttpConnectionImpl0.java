package com.treasure_data.client;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;

@Ignore
public class MockHttpConnectionImpl0 extends HttpClientAdaptor.HttpConnectionImpl {

    public MockHttpConnectionImpl0() {
        super();
    }

//    @Override
//    void doGetRequest(String path, Map<String, String> header, Map<String, String> params)
//            throws IOException {
//        Properties props = System.getProperties();
//        String host = props.getProperty(
//                Config.TD_API_SERVER_HOST, Config.TD_API_SERVER_HOST_DEFAULT);
//        int port = Integer.parseInt(props.getProperty(
//                Config.TD_API_SERVER_PORT, Config.TD_API_SERVER_PORT_DEFAULT));
//
//        StringBuilder sbuf = new StringBuilder();
//        sbuf.append("http://").append(host).append(":").append(port).append(path);
//
//        // parameters
//        if (params != null && !params.isEmpty()) {
//            sbuf.append("?");
//            int paramSize = params.size();
//            Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
//            for (int i = 0; i < paramSize; ++i) {
//                Map.Entry<String, String> e = iter.next();
//                sbuf.append(e.getKey()).append("=").append(e.getValue());
//                if (i + 1 != paramSize) {
//                    sbuf.append("&");
//                }
//            }
//        }
//
//        // create connection object with url
//        URL url = new URL(sbuf.toString()); // TODO #MN should use URL class for encoding 
//        conn = (HttpURLConnection) url.openConnection();
//
//        // header
//        conn.setRequestMethod("GET");
//        if (apiKey != null) {
//            conn.setRequestProperty("Authorization", "TD1 " + apiKey);
//        }
//        conn.setRequestProperty("Date", toRFC2822Format(new Date()));
//        if (header != null && !header.isEmpty()) {
//            for (Map.Entry<String, String> e : header.entrySet()) {
//                conn.setRequestProperty(e.getKey(), e.getValue());
//            }
//        }
//
//        // do connection to server
//        conn.connect();
//    }
//
//    @Override
//    void doPostRequest(String path, Map<String, String> header, Map<String, String> params)
//            throws IOException {
//        Properties props = System.getProperties();
//        String host = props.getProperty(Config.TD_API_SERVER_HOST,
//                Config.TD_API_SERVER_HOST_DEFAULT);
//        int port = Integer.parseInt(props.getProperty(
//                Config.TD_API_SERVER_PORT, Config.TD_API_SERVER_PORT_DEFAULT));
//
//        StringBuilder sbuf = new StringBuilder();
//        sbuf.append("http://").append(host).append(":").append(port).append(path);
//
//        // parameters
//        if (params != null && !params.isEmpty()) {
//            sbuf.append("?");
//            int paramSize = params.size();
//            Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
//            for (int i = 0; i < paramSize; ++i) {
//                Map.Entry<String, String> e = iter.next();
//                sbuf.append(e.getKey()).append("=").append(e.getValue());
//                if (i + 1 != paramSize) {
//                    sbuf.append("&");
//                }
//            }
//            URL url = new URL(sbuf.toString()); // TODO #MN should use URL class for encoding
//            conn = (HttpURLConnection) url.openConnection();
//        } else {
//            URL url = new URL(sbuf.toString()); // TODO #MN should use URL class for encoding
//            conn = (HttpURLConnection) url.openConnection();
//            conn.setRequestProperty("Content-Length", "0");
//        }
//
//        // header
//        conn.setRequestMethod("POST");
//        if (apiKey != null) {
//            conn.setRequestProperty("Authorization", "TD1 " + apiKey);
//        }
//        conn.setRequestProperty("Date", toRFC2822Format(new Date()));
//        if (header != null && !header.isEmpty()) {
//            for (Map.Entry<String, String> e : header.entrySet()) {
//                conn.setRequestProperty(e.getKey(), e.getValue());
//            }
//        }
//        conn.connect();
//    }
//
//    @Override
//    HttpURLConnection doPutRequest(String path, byte[] bytes) throws IOException {
//        Properties props = System.getProperties();
//        String host = props.getProperty(
//                Config.TD_API_SERVER_HOST, Config.TD_API_SERVER_HOST_DEFAULT);
//        int port = Integer.parseInt(props.getProperty(
//                Config.TD_API_SERVER_PORT, Config.TD_API_SERVER_PORT_DEFAULT));
//
//        StringBuilder sbuf = new StringBuilder();
//        sbuf.append("http://").append(host).append(":").append(port).append(path);
//
//        URL url = new URL(sbuf.toString()); // TODO #MN should use URL class for encoding 
//        conn = (HttpURLConnection) url.openConnection();
//        conn.setReadTimeout(600 * 1000);
//        conn.setRequestMethod("PUT");
//        //conn.setRequestProperty("Content-Type", "application/octet-stream");
//        conn.setRequestProperty("Content-Length", "" + bytes.length);
//        if (apiKey != null) {
//            conn.setRequestProperty("Authorization", "TD1 " + apiKey);
//        }
//        conn.setRequestProperty("Date", toRFC2822Format(new Date()));
//        conn.setDoOutput(true);
//        conn.setUseCaches (false);
//        //conn.connect();
//
//        // body
//        BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
//        out.write(bytes);
//        out.flush();
//        //out.close();
//
//        return conn;
//    }
//
//    int getResponseCode() throws IOException {
//        return conn.getResponseCode();
//    }
//
//    String getResponseMessage() throws IOException {
//        return conn.getResponseMessage();
//    }
//
//    String getResponseBody() throws IOException {
//        StringBuilder sbuf = new StringBuilder();
//        BufferedReader reader = new BufferedReader( 
//                new InputStreamReader(conn.getInputStream()));
//        while (true){
//            String line = reader.readLine();
//            if ( line == null ){
//                break;
//            }
//            sbuf.append(line);
//        }
//        reader.close();
//        return sbuf.toString();
//    }
//
//    void disconnect() {
//        conn.disconnect();
//    }
//
//    private static String toRFC2822Format(Date from) {
//        return RFC2822FORMAT.format(from);
//    }
}
