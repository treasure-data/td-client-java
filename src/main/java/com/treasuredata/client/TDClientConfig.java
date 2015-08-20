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
     * Keys for configuring TDClient with a properties file (of System properties)
     */
    public static final String TD_CLIENT_APIKEY = "td.client.apikey";
    public static final String TD_CLIENT_USER = "td.client.user";
    public static final String TD_CLIENT_PASSOWRD = "td.client.password";
    public static final String TD_CLIENT_USESSL = "td.client.usessl";
    public static final String TD_CLIENT_API_ENDPOINT = "td.client.endpoint";
    public static final String TD_CLIENT_API_PORT = "td.client.port";
    public static final String TD_CLIENT_API_READ_TIMEOUT = "td.client.read-timeout";
    public static final String TD_CLIENT_API_IDLE_TIMEOUT = "td.client.idle-timeout";
    public static final String TD_CLIENT_RETRY_LIMIT = "td.client.retry.limit";
    public static final String TD_CLIENT_RETRY_INITIAL_WAIT_MILLIS = "td.client.retry.initial-wait";
    public static final String TD_CLIENT_RETRY_INTERVAL_MILLIS = "td.client.retry.interval";

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

    public static <V> V checkNotNull(V v, String message)
            throws TDClientException
    {
        if (v == null) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_CONFIGURATION, message);
        }
        return v;
    }

    /**
     * Get the default TDClientConfig by reading $HOME/.td/td.conf file.
     * @return
     * @throws IOException
     * @throws TDClientException
     */
    public static TDClientConfig currentConfig()
            throws IOException, TDClientException
    {
        Properties p = readTDConf();
        String apiKey = firstNonNull(p.getProperty("apikey"), System.getenv().get(TD_CLIENT_APIKEY));
        return new Builder().setApiKey(apiKey).result();
    }

    /**
     * Return a new configuration with a given TD API key
     * @param apiKey
     * @return
     */
    public TDClientConfig withApiKey(String apiKey) {
        return new TDClientConfig(
                Optional.of(endpoint),
                Optional.of(port),
                useSSL,
                Optional.of(apiKey),
                proxy,
                retryLimit,
                retryInitialWaitMillis,
                retryInitialWaitMillis);
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
            int retryIntervalMillis
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
            throws IOException
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
            throws IOException
    {
        Properties p = new Properties();
        logger.info(String.format("Reading configuration file: %s", file));
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

    private static String firstNonNull(Object... keys)
    {
        if (keys != null) {
            for (Object k : keys) {
                if (k != null) {
                    return k.toString();
                }
            }
        }
        return "";
    }

//    public Properties toProperties() {
//        Properties prop = new Properties();
//        prop.setProperty(Config.TD_CK_API_SERVER_SCHEME, scheme);
//        prop.setProperty(Config.TD_JDBC_USESSL, Boolean.toString(useSSL));
//        prop.setProperty(Config.TD_API_SERVER_HOST, endpoint);
//        prop.setProperty(Config.TD_API_SERVER_PORT, Integer.toString(port));
//
//        if(apiKey.isDefined()) {
//            prop.setProperty(Config.TD_API_KEY, apiKey.get());
//        }
//
//        if(proxy.isDefined()) {
//            Properties proxyProp = proxy.get().toProperties();
//            prop.putAll(proxyProp);
//        }
//        return prop;
//    }

    public static class Builder {
        private Optional<String> endpoint = Optional.absent();
        private Optional<Integer> port = Optional.absent();
        private boolean useSSL = false;
        private Optional<String> apiKey = Optional.absent();
        private Optional<ProxyConfig> proxy = Optional.absent();

        private int retryLimit = 7;
        private int retryInitialWaitMillis = 1000;
        private final int retryIntervalMillis = 2000;

        public Builder() {}

        public Builder(TDClientConfig config) {
            this.endpoint = Optional.of(config.endpoint);
            this.port = Optional.of(config.port);
            this.useSSL = config.useSSL;
            this.proxy = config.proxy;
        }

        public Builder setEndpoint(String endpoint) {
            this.endpoint = Optional.of(endpoint);
            return this;
        }

        public Builder setPort(int port) {
            this.port = Optional.of(port);
            return this;
        }

        public Builder setUseSSL(boolean useSSL) {
            this.useSSL = useSSL;
            return this;
        }

        public Builder setApiKey(String apiKey) {
            this.apiKey = Optional.of(apiKey);
            return this;
        }

        public Builder unsetApiKey() {
            this.apiKey = Optional.absent();
            return this;
        }

        public Builder setProxyConfig(ProxyConfig proxyConfig) {
            this.proxy = Optional.of(proxyConfig);
            return this;
        }

        public TDClientConfig result() {
            return new TDClientConfig(
                    endpoint,
                    port,
                    useSSL,
                    apiKey,
                    proxy,
                    retryLimit,
                    retryInitialWaitMillis,
                    retryIntervalMillis
            );
        }
    }

}
