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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

/**
 * TD Client configuration
 */
public class TDClientConfig
{
    public static final String ENV_TD_CLIENT_APIKEY = "TD_API_KEY";
    /**
     * Keys for configuring TDClient with a properties file (or System properties)
     */
    public static final String TD_CLIENT_APIKEY = "td.client.apikey";
    public static final String TD_CLIENT_USER = "td.client.user";
    public static final String TD_CLIENT_PASSOWRD = "td.client.password";
    public static final String TD_CLIENT_USESSL = "td.client.usessl";
    public static final String TD_CLIENT_API_ENDPOINT = "td.client.endpoint";
    public static final String TD_CLIENT_API_PORT = "td.client.port";
    public static final String TD_CLIENT_RETRY_LIMIT = "td.client.retry.limit";
    public static final String TD_CLIENT_RETRY_INITIAL_WAIT_MILLIS = "td.client.retry.initial-wait";
    public static final String TD_CLIENT_RETRY_INTERVAL_MILLIS = "td.client.retry.interval";
    public static final String TD_CLIENT_CONNECT_TIMEOUT_MILLIS = "td.client.connect-timeout";
    public static final String TD_CLIENT_IDLE_TIMEOUT_MILLIS = "td.client.idle-timeout";
    public static final String TD_CLIENT_MAX_CONNECTIONS_PER_DESTINATION = "td.client.max-connections-per-destination";
    public static final String TD_CLIENT_PROXY_HOST = "td.client.proxy.host";
    public static final String TD_CLIENT_PROXY_PORT = "td.client.proxy.port";
    public static final String TD_CLIENT_PROXY_USER = "td.client.proxy.user";
    public static final String TD_CLIENT_PROXY_PASSWORD = "td.client.proxy.password";
    private static Logger logger = LoggerFactory.getLogger(TDClientConfig.class);
    /**
     * endpoint URL (e.g., api.treasuredata.com, api-staging.treasuredata.com)
     */
    private final String endpoint;
    private final int port;
    private final Optional<String> apiKey;
    private final Optional<ProxyConfig> proxy;
    private final String httpScheme;
    private final boolean useSSL;
    private final int retryLimit;
    private final int retryInitialWaitMillis;
    private final int retryIntervalMillis;
    private final int connectTimeoutMillis;
    private final int idleTimeoutMillis;
    private final int maxConnectionsPerDestination;

    public static <V> V checkNotNull(V v, String message)
            throws TDClientException
    {
        if (v == null) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_CONFIGURATION, message);
        }
        return v;
    }

    private static String findNonNull(Object... keys)
    {
        if (keys != null) {
            for (Object k : keys) {
                if (k != null) {
                    return k.toString();
                }
            }
        }
        return null;
    }

    private static TDClientConfig currentConfig;

    /**
     * Get the default TDClientConfig by reading $HOME/.td/td.conf file.
     *
     * @return
     * @throws IOException
     * @throws TDClientException
     */
    public static TDClientConfig currentConfig()
            throws TDClientException
    {
        if (currentConfig == null) {
            Properties p = readTDConf();
            String apiKey = findNonNull(System.getenv().get(ENV_TD_CLIENT_APIKEY), System.getProperty(TD_CLIENT_APIKEY), p.getProperty("apikey"));
            if (apiKey == null || apiKey.isEmpty()) {
                currentConfig = new Builder().result();
            }
            else {
                currentConfig = new Builder().setApiKey(apiKey).result();
            }
        }
        return currentConfig;
    }

    /**
     * Return a new configuration with a given TD API key
     *
     * @param apiKey
     * @return
     */
    public TDClientConfig withApiKey(String apiKey)
    {
        return new Builder(this).setApiKey(apiKey).result();
    }

    public TDClientConfig withProxy(ProxyConfig proxy)
    {
        return new Builder(this).setProxyConfig(proxy).result();
    }

    @JsonCreator
    public TDClientConfig(
            Optional<String> endpoint,
            Optional<Integer> port,
            boolean useSSL,
            Optional<String> apiKey,
            Optional<ProxyConfig> proxy,
            int retryLimit,
            int retryInitialWaitMillis,
            int retryIntervalMillis,
            int connectTimeoutMillis,
            int idleTimeoutMillis,
            int maxConnectionsPerDestination
    )
    {
        this.httpScheme = useSSL ? "https://" : "http://";
        this.endpoint = endpoint.or("api.treasuredata.com");
        this.port = port.or(useSSL ? 443 : 80);
        this.useSSL = useSSL;
        this.apiKey = apiKey;
        this.proxy = proxy;
        this.retryLimit = retryLimit;
        this.retryInitialWaitMillis = retryInitialWaitMillis;
        this.retryIntervalMillis = retryIntervalMillis;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.maxConnectionsPerDestination = maxConnectionsPerDestination;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public Optional<String> getApiKey()
    {
        return apiKey;
    }

    public int getRetryLimit()
    {
        return retryLimit;
    }

    public int getRetryInitialWaitMillis()
    {
        return retryInitialWaitMillis;
    }

    public int getRetryIntervalMillis()
    {
        return retryIntervalMillis;
    }

    public int getConnectTimeoutMillis()
    {
        return connectTimeoutMillis;
    }

    public int getIdleTimeoutMillis()
    {
        return idleTimeoutMillis;
    }

    public int getMaxConnectionsPerDestination()
    {
        return maxConnectionsPerDestination;
    }

    public int getPort()
    {
        return port;
    }

    public Optional<ProxyConfig> getProxy()
    {
        return proxy;
    }

    public String getHttpScheme()
    {
        return httpScheme;
    }

    public boolean isUseSSL()
    {
        return useSSL;
    }

    /**
     * Read user user (e-mail address and password properties from $HOME/.td/td.conf
     *
     * @return
     * @throws IOException
     */
    public static Properties readTDConf()
    {
        Properties p = new Properties();
        File file = new File(System.getProperty("user.home", "./"), String.format(".td/td.conf"));
        if (!file.exists()) {
            logger.warn(String.format("config file %s is not found", file));
            return p;
        }
        return readTDConf(file);
    }

    public static Properties readTDConf(File file)
    {
        Properties p = new Properties();
        logger.info(String.format("Reading configuration file: %s", file));
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            StringBuilder extracted = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.startsWith("[") || trimmed.startsWith("#")) {
                    continue; // skip [... ] line or comment line
                }
                extracted.append(line.trim());
                extracted.append("\n");
            }
            String props = extracted.toString();
            p.load(new StringReader(props));
            return p;
        }
        catch (IOException e) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_CONFIGURATION, String.format("Failed to read config file: %s", file), e);
        }
    }

    public static class Builder
    {
        private Optional<String> endpoint = Optional.absent();
        private Optional<Integer> port = Optional.absent();
        private boolean useSSL = false;
        private Optional<String> apiKey = Optional.absent();
        private Optional<ProxyConfig> proxy = Optional.absent();
        private int retryLimit = 7;
        private int retryInitialWaitMillis = 1000;
        private int retryIntervalMillis = 2000;
        private int connectTimeoutMillis = 15000;
        private int idleTimeoutMillis = 60000;
        private int maxConnectionsPerDestination = 64;

        public Builder()
        {
        }

        public Builder(TDClientConfig config)
        {
            this.endpoint = Optional.of(config.endpoint);
            this.port = Optional.of(config.port);
            this.useSSL = config.useSSL;
            this.apiKey = config.apiKey;
            this.proxy = config.proxy;
            this.retryLimit = config.retryLimit;
            this.retryInitialWaitMillis = config.retryInitialWaitMillis;
            this.retryIntervalMillis = config.retryIntervalMillis;
            this.connectTimeoutMillis = config.connectTimeoutMillis;
            this.idleTimeoutMillis = config.idleTimeoutMillis;
            this.maxConnectionsPerDestination = config.maxConnectionsPerDestination;
        }

        public Builder setEndpoint(String endpoint)
        {
            this.endpoint = Optional.of(endpoint);
            return this;
        }

        public Builder setPort(int port)
        {
            this.port = Optional.of(port);
            return this;
        }

        public Builder setUseSSL(boolean useSSL)
        {
            this.useSSL = useSSL;
            return this;
        }

        public Builder setApiKey(String apiKey)
        {
            this.apiKey = Optional.of(apiKey);
            return this;
        }

        public Builder unsetApiKey()
        {
            this.apiKey = Optional.absent();
            return this;
        }

        public Builder setProxyConfig(ProxyConfig proxyConfig)
        {
            this.proxy = Optional.of(proxyConfig);
            return this;
        }

        public TDClientConfig result()
        {
            return new TDClientConfig(
                    endpoint,
                    port,
                    useSSL,
                    apiKey,
                    proxy,
                    retryLimit,
                    retryInitialWaitMillis,
                    retryIntervalMillis,
                    connectTimeoutMillis,
                    idleTimeoutMillis,
                    maxConnectionsPerDestination
            );
        }
    }
}
