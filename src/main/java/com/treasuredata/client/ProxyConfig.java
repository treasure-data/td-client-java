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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Proxy configuration to access TD API
 */
public class ProxyConfig
{
    private final String host;
    private final int port;
    private final boolean useSSL;
    private final Optional<String> user;
    private final Optional<String> password;

    public ProxyConfig(String host, int port, boolean useSSL, Optional<String> user, Optional<String> password)
    {
        this.host = host;
        this.port = port;
        this.useSSL = useSSL;
        this.user = user;
        this.password = password;
    }

    public URI getUri()
    {
        String protocol = useSSL ? "https" : "http";
        String url = String.format("%s://%s:%s", protocol, host, port);
        try {
            return new URI(url);
        }
        catch (URISyntaxException e) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_CONFIGURATION, "invalid proxy url: " + url);
        }
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public Optional<String> getUser()
    {
        return user;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    public boolean useSSL()
    {
        return useSSL;
    }

    public boolean requireAuthentication()
    {
        return user.isPresent() || password.isPresent();
    }

    @Override
    public String toString()
    {
        return "ProxyConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", useSSL=" + useSSL +
                ", user=" + user +
                ", password=" + password +
                '}';
    }

    public static class ProxyConfigBuilder
    {
        private String host = "localhost";
        private int port = 8080;
        private boolean useSSL = false;
        private Optional<String> user = Optional.empty();
        private Optional<String> password = Optional.empty();

        public ProxyConfigBuilder()
        {
        }

        public ProxyConfigBuilder(ProxyConfig config)
        {
            this.host = config.host;
            this.port = config.port;
            this.useSSL = config.useSSL;
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

        public void useSSL(boolean useSSL)
        {
            this.useSSL = useSSL;
        }

        public ProxyConfigBuilder setUser(String user)
        {
            this.user = Optional.of(user);
            return this;
        }

        public ProxyConfigBuilder setPassword(String password)
        {
            this.password = Optional.of(password);
            return this;
        }

        public ProxyConfig createProxyConfig()
        {
            return new ProxyConfig(host, port, useSSL, user, password);
        }
    }
}
