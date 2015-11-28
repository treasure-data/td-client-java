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
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/**
 * TD Client configuration
 */
public class TDClientConfig
{
    private static Logger logger = LoggerFactory.getLogger(TDClientConfig.class);
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
    public static final String TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS = "td.client.retry.initial-interval";
    public static final String TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS = "td.client.retry.max-interval";
    public static final String TD_CLIENT_RETRY_MULTIPLIER = "td.client.retry.multiplier";
    public static final String TD_CLIENT_CONNECT_TIMEOUT_MILLIS = "td.client.connect-timeout";
    public static final String TD_CLIENT_IDLE_TIMEOUT_MILLIS = "td.client.idle-timeout";
    public static final String TD_CLIENT_CONNECTION_POOL_SIZE = "td.client.connection-pool-size";
    public static final String TD_CLIENT_PROXY_HOST = "td.client.proxy.host";
    public static final String TD_CLIENT_PROXY_PORT = "td.client.proxy.port";
    public static final String TD_CLIENT_PROXY_USER = "td.client.proxy.user";
    public static final String TD_CLIENT_PROXY_USESSL = "td.client.proxy.usessl";
    public static final String TD_CLIENT_PROXY_PASSWORD = "td.client.proxy.password";
    /**
     * endpoint URL (e.g., api.treasuredata.com, ybi.jp-east.idcfcloud.com)
     */
    public final String endpoint;
    public final Optional<Integer> port;
    public final Optional<String> apiKey;
    public final Optional<String> user;
    public final Optional<String> password;
    public final Optional<ProxyConfig> proxy;
    public final String httpScheme;
    public final boolean useSSL;
    public final int retryLimit;
    public final int retryInitialIntervalMillis;
    public final int retryMaxIntervalMillis;
    public final double retryMultiplier;
    public final int connectTimeoutMillis;
    public final int idleTimeoutMillis;
    public final int connectionPoolSize;
    public static Properties tdConf;

    public TDClientConfig withProxy(ProxyConfig proxy)
    {
        return new Builder(this).setProxyConfig(proxy).build();
    }

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
        return newBuilder().build();
    }

    public static Builder newBuilder()
    {
        if (tdConf == null) {
            tdConf = readTDConf();
        }
        return new Builder(tdConf);
    }

    /**
     * Create a new td-client configuration initialized with System Properties and the given properties for the default values.
     * Precedence of properties is the following order:
     * <ol>
     * <li>System Properties</li>
     * <li>Given Properties Object</li>
     * </ol>
     *
     * @param p the default values
     * @return
     */
    public static TDClientConfig newConfig(Properties p)
    {
        Builder b = new Builder(p);
        return b.build();
    }

    @JsonCreator
    public TDClientConfig(
            Optional<String> endpoint,
            Optional<Integer> port,
            boolean useSSL,
            Optional<String> apiKey,
            Optional<String> user,
            Optional<String> password,
            Optional<ProxyConfig> proxy,
            int retryLimit,
            int retryInitialIntervalMillis,
            int retryMaxIntervalMillis,
            double retryMultiplier,
            int connectTimeoutMillis,
            int idleTimeoutMillis,
            int connectionPoolSize
    )
    {
        this.httpScheme = useSSL ? "https://" : "http://";
        this.endpoint = endpoint.or("api.treasuredata.com");
        this.port = port;
        this.useSSL = useSSL;
        this.apiKey = apiKey;
        this.user = user;
        this.password = password;
        this.proxy = proxy;
        this.retryLimit = retryLimit;
        this.retryInitialIntervalMillis = retryInitialIntervalMillis;
        this.retryMaxIntervalMillis = retryMaxIntervalMillis;
        this.retryMultiplier = retryMultiplier;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.connectionPoolSize = connectionPoolSize;
    }

    /**
     * Read apikey, user (e-mail address) and password from $HOME/.td/td.conf
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
            List<String> lines = Files.readLines(file, StandardCharsets.UTF_8);
            StringBuilder extracted = new StringBuilder();
            for (String line : lines) {
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

    public static String firstNonNull(String... values)
    {
        for (String v : values) {
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    public static class Builder
    {
        private Optional<String> endpoint = Optional.absent();
        private Optional<Integer> port = Optional.absent();
        private boolean useSSL;
        private Optional<String> apiKey = Optional.absent();
        private Optional<String> user = Optional.absent();
        private Optional<String> password = Optional.absent();
        private Optional<ProxyConfig> proxy = Optional.absent();
        private int retryLimit;
        private int retryInitialIntervalMillis;
        private int retryMaxIntervalMillis;
        private double retryMultiplier;
        private int connectTimeoutMillis;
        private int idleTimeoutMillis;
        private int connectionPoolSize;

        private static Optional<String> getConfigProperty(String key, Properties defaultProperty)
        {
            return Optional.fromNullable(firstNonNull(System.getProperty(key), defaultProperty.getProperty(key)));
        }

        private static Optional<Integer> getConfigPropertyInt(String key, Properties defaultProperty)
        {
            String v = firstNonNull(System.getProperty(key), defaultProperty.getProperty(key));
            if (v != null) {
                try {
                    return Optional.of(Integer.parseInt(v));
                }
                catch (NumberFormatException e) {
                    throw new TDClientException(TDClientException.ErrorType.INVALID_CONFIGURATION, String.format("[%s] cannot cast %s to integer", key, v));
                }
            }
            else {
                return Optional.absent();
            }
        }

        private static Optional<Double> getConfigPropertyDouble(String key, Properties defaultProperty)
        {
            String v = firstNonNull(System.getProperty(key), defaultProperty.getProperty(key));
            if (v != null) {
                try {
                    return Optional.of(Double.parseDouble(v));
                }
                catch (NumberFormatException e) {
                    throw new TDClientException(TDClientException.ErrorType.INVALID_CONFIGURATION, String.format("[%s] cannot cast %s to double", key, v));
                }
            }
            else {
                return Optional.absent();
            }
        }

        Builder()
        {
            this(new Properties());
        }

        /***
         * Constructor used for copying and overwriting an existing configuration
         *
         * @param config
         */
        Builder(TDClientConfig config)
        {
            this.endpoint = Optional.of(config.endpoint);
            this.port = config.port;
            this.useSSL = config.useSSL;
            this.apiKey = config.apiKey;
            this.user = config.user;
            this.password = config.password;
            this.proxy = config.proxy;
            this.retryLimit = config.retryLimit;
            this.retryInitialIntervalMillis = config.retryInitialIntervalMillis;
            this.retryMaxIntervalMillis = config.retryMaxIntervalMillis;
            this.retryMultiplier = config.retryMultiplier;
            this.connectTimeoutMillis = config.connectTimeoutMillis;
            this.idleTimeoutMillis = config.idleTimeoutMillis;
            this.connectionPoolSize = config.connectionPoolSize;
        }

        /**
         * Populate a builder populated with the default values. Precedence of the default values is:
         * - Environment variable
         * - System property
         * - Given Property object
         *
         * @param defaultValues
         */
        Builder(Properties defaultValues)
        {
            this.endpoint = getConfigProperty(TD_CLIENT_API_ENDPOINT, defaultValues);
            this.port = getConfigPropertyInt(TD_CLIENT_API_PORT, defaultValues);
            this.useSSL = Boolean.parseBoolean(getConfigProperty(TD_CLIENT_USESSL, defaultValues).or("true"));

            // For APIKEY we read the environment variable.
            // We also read apikey, user and password specified in td.conf file
            this.apiKey = Optional.fromNullable(System.getenv(ENV_TD_CLIENT_APIKEY))
                    .or(getConfigProperty(TD_CLIENT_APIKEY, defaultValues))
                    .or(Optional.fromNullable(defaultValues.getProperty("apikey")));
            this.user = getConfigProperty(TD_CLIENT_USER, defaultValues)
                    .or(Optional.fromNullable(defaultValues.getProperty("user")));
            this.password = getConfigProperty(TD_CLIENT_PASSOWRD, defaultValues)
                    .or(Optional.fromNullable(defaultValues.getProperty("password")));

            // proxy
            Optional<String> proxyHost = getConfigProperty(TD_CLIENT_PROXY_HOST, defaultValues);
            Optional<Integer> proxyPort = getConfigPropertyInt(TD_CLIENT_PROXY_PORT, defaultValues);
            Optional<String> proxyUseSSL = getConfigProperty(TD_CLIENT_PROXY_USESSL, defaultValues);
            Optional<String> proxyUser = getConfigProperty(TD_CLIENT_PROXY_USER, defaultValues);
            Optional<String> proxyPassword = getConfigProperty(TD_CLIENT_PROXY_PASSWORD, defaultValues);
            boolean hasProxy = false;
            ProxyConfig.ProxyConfigBuilder proxyConfig = new ProxyConfig.ProxyConfigBuilder();
            if (proxyHost.isPresent()) {
                hasProxy = true;
                proxyConfig.setHost(proxyHost.get());
            }
            if (proxyPort.isPresent()) {
                hasProxy = true;
                proxyConfig.setPort(proxyPort.get());
            }
            if (proxyUseSSL.isPresent()) {
                hasProxy = true;
                proxyConfig.useSSL(Boolean.parseBoolean(proxyUseSSL.get()));
            }
            if (proxyUser.isPresent()) {
                hasProxy = true;
                proxyConfig.setUser(proxyUser.get());
            }
            if (proxyPassword.isPresent()) {
                hasProxy = true;
                proxyConfig.setPassword(proxyPassword.get());
            }
            this.proxy = Optional.fromNullable(hasProxy ? proxyConfig.createProxyConfig() : null);

            // http client parameter
            this.retryLimit = getConfigPropertyInt(TD_CLIENT_RETRY_LIMIT, defaultValues).or(7);
            this.retryInitialIntervalMillis = getConfigPropertyInt(TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS, defaultValues).or(500);
            this.retryMaxIntervalMillis = getConfigPropertyInt(TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS, defaultValues).or(60000);
            this.retryMultiplier = getConfigPropertyDouble(TD_CLIENT_RETRY_MULTIPLIER, defaultValues).or(2.0);
            this.connectTimeoutMillis = getConfigPropertyInt(TD_CLIENT_CONNECT_TIMEOUT_MILLIS, defaultValues).or(15000);
            this.idleTimeoutMillis = getConfigPropertyInt(TD_CLIENT_IDLE_TIMEOUT_MILLIS, defaultValues).or(60000);
            this.connectionPoolSize = getConfigPropertyInt(TD_CLIENT_CONNECTION_POOL_SIZE, defaultValues).or(64);
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

        public Builder setUser(String user)
        {
            this.user = Optional.of(user);
            return this;
        }

        public Builder setPassword(String password)
        {
            this.password = Optional.of(password);
            return this;
        }

        public Builder setProxyConfig(ProxyConfig proxyConfig)
        {
            this.proxy = Optional.of(proxyConfig);
            return this;
        }

        public Builder setRetryLimit(int retryLimit)
        {
            this.retryLimit = retryLimit;
            return this;
        }

        public Builder setRetryInitialIntervalMillis(int retryInitialIntervalMillis)
        {
            this.retryInitialIntervalMillis = retryInitialIntervalMillis;
            return this;
        }

        public Builder setRetryMaxIntervalMillis(int retryMaxIntervalMillis)
        {
            this.retryMaxIntervalMillis = retryMaxIntervalMillis;
            return this;
        }

        public void setRetryMultiplier(double retryMultiplier)
        {
            this.retryMultiplier = retryMultiplier;
        }

        public Builder setConnectTimeoutMillis(int connectTimeoutMillis)
        {
            this.connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        public Builder setIdleTimeoutMillis(int idleTimeoutMillis)
        {
            this.idleTimeoutMillis = idleTimeoutMillis;
            return this;
        }

        public Builder setConnectionPoolSize(int connectionPoolSize)
        {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        public TDClientConfig build()
        {
            return new TDClientConfig(
                    endpoint,
                    port,
                    useSSL,
                    apiKey,
                    user,
                    password,
                    proxy,
                    retryLimit,
                    retryInitialIntervalMillis,
                    retryMaxIntervalMillis,
                    retryMultiplier,
                    connectTimeoutMillis,
                    idleTimeoutMillis,
                    connectionPoolSize
            );
        }
    }
}
