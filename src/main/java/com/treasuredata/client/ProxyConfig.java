/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.client;

import java.util.Properties;

/**
 * Proxy configuration to access TD API
 */
public class ProxyConfig
{
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    public ProxyConfig(String host, int port, String user, String password)
    {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public void apply()
    {
        System.setProperty("http.proxyHost", host);
        System.setProperty("https.proxyHost", host);
        String portStr = Integer.toString(port);
        System.setProperty("http.proxyPort", portStr);
        System.setProperty("https.proxyPort", portStr);
        System.setProperty("http.proxyUser", user);
        System.setProperty("http.proxyPassword", password);
    }

    public Properties toProperties()
    {
        Properties prop = new Properties();
        prop.setProperty("http.proxyHost", host);
        prop.setProperty("https.proxyHost", host);

        String portStr = Integer.toString(port);
        prop.setProperty("http.proxyPort", portStr);
        prop.setProperty("https.proxyPort", portStr);

        prop.setProperty("http.proxyUser", user);
        prop.setProperty("http.proxyPassword", password);
        return prop;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    @Override
    public String toString()
    {
        return "ProxyConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", user='" + user + '\'' +
                '}';
    }

    public static class ProxyConfigBuilder
    {
        private String host;
        private int port = 8080;
        private String user;
        private String password;

        public ProxyConfigBuilder()
        {
        }

        public ProxyConfigBuilder(ProxyConfig config)
        {
            this.host = config.host;
            this.port = config.port;
            this.user = config.user;
            this.password = config.password;
        }

        public ProxyConfigBuilder setHost(String host)
        {
            this.host = host;
            return this;
        }

        public ProxyConfigBuilder setPort(int port)
        {
            this.port = port;
            return this;
        }

        public ProxyConfigBuilder setUser(String user)
        {
            this.user = user;
            return this;
        }

        public ProxyConfigBuilder setPassword(String password)
        {
            this.password = password;
            return this;
        }

        public ProxyConfig createProxyConfig()
        {
            return new ProxyConfig(host, port, user, password);
        }
    }
}
