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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import com.treasure_data.auth.TreasureDataCredentials;

public abstract class AbstractClientAdaptor {
    private static final String userAgentKey = "User-Agent";
    private Config conf;

    protected HttpConnectionImpl conn = null;
    protected String userAgent = null;

    public AbstractClientAdaptor(Config conf) {
        this.conf = conf;
    }

    public Config getConfig() {
        return conf;
    }

    public TreasureDataCredentials getTreasureDataCredentials() {
        return conf.getCredentials();
    }

    public void setTreasureDataCredentials(TreasureDataCredentials credentials) {
        conf.setCredentials(credentials);
    }

    protected HttpConnectionImpl getConnection() {
        return conn;
    }

    protected HttpConnectionImpl createConnection() {
        if (conn == null) {
            conn = new HttpConnectionImpl(getConfig().getProperties());
        }
        return conn;
    }

    protected void setConnection(HttpConnectionImpl conn) {
        this.conn = conn;
    }

    public int getRetryCount() {
        String count = this.getConfig().getProperties().getProperty(
                Config.TD_CLIENT_RETRY_COUNT, Config.TD_CLIENT_RETRY_COUNT_DEFAULTVALUE);
        return Integer.parseInt(count);
    }

    public long getRetryWaitTime() {
        String time = this.getConfig().getProperties().getProperty(
                Config.TD_CLIENT_RETRY_WAIT_TIME, Config.TD_CLIENT_RETRY_WAIT_TIME_DEFAULTVALUE);
        return Long.parseLong(time);
    }

    protected void waitRetry(long time, int retryCount) {
        try {
            Thread.sleep(time * (long) Math.pow(2.0, (double) retryCount) );
        } catch (InterruptedException e) {
            // ignore
        }
    }

    protected static String getJobID(Map<String, Object> map) {
        Object job_id = map.get("job_id");
        if (job_id instanceof Number) {
            return ((Number) job_id).toString();
        } else {
            return (String) job_id;
        }
    }

    public void setUserAgentHeader(Map<String, String> header) {
        if (userAgent == null) {
            userAgent = "TD-Client-Java " + getVersion();
        }

        header.put(userAgentKey, userAgent);
    }

    protected static String getVersion() {
        String version = "";

        Class<TreasureDataClient> c = TreasureDataClient.class;
        String className = c.getSimpleName() + ".class";
        String classPath = c.getResource(className).toString();

        if (!classPath.startsWith("jar")) {
            return version;
        }

        String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) +
                "/META-INF/MANIFEST.MF";
        try {
            Manifest manifest = new Manifest(new URL(manifestPath).openStream());
            Attributes attr = manifest.getMainAttributes();
            if ((version = attr.getValue("Implementation-Version")) != null) {
                return version;
            } else {
                return "";
            }
        } catch (IOException e) {
            return version;
        }
    }

}
