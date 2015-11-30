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

import com.google.common.base.Optional;

import java.util.Properties;

import static com.treasuredata.client.TDClientConfig.ENV_TD_CLIENT_APIKEY;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_APIKEY;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_API_ENDPOINT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_API_PORT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_CONNECTION_POOL_SIZE;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_CONNECT_TIMEOUT_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_IDLE_TIMEOUT_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PASSOWRD;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_HOST;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_PASSWORD;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_PORT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_USER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_PROXY_USESSL;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_LIMIT;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_RETRY_MULTIPLIER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_USER;
import static com.treasuredata.client.TDClientConfig.TD_CLIENT_USESSL;
import static com.treasuredata.client.TDClientConfig.getTDConfProperties;

/**
 *
 */
public class TDClientBuilder
{
    private Optional<String> endpoint = Optional.absent();
    private Optional<Integer> port = Optional.absent();
    private boolean useSSL = true;
    private Optional<String> apiKey = Optional.absent();
    private Optional<String> user = Optional.absent();
    private Optional<String> password = Optional.absent();
    private Optional<ProxyConfig> proxy = Optional.absent();
    private int retryLimit = 7;
    private int retryInitialIntervalMillis = 500;
    private int retryMaxIntervalMillis = 60000;
    private double retryMultiplier = 2.0;
    private int connectTimeoutMillis = 15000;
    private int idleTimeoutMillis = 60000;
    private int connectionPoolSize = 64;

    private static Optional<String> getConfigProperty(Properties p, String key)
    {
        return Optional.fromNullable(p.getProperty(key));
    }

    private static Optional<Integer> getConfigPropertyInt(Properties p, String key)
    {
        String v = p.getProperty(key);
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

    private static Optional<Double> getConfigPropertyDouble(Properties p, String key)
    {
        String v = p.getProperty(key);
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

    /**
     * Create a new TDClinenb builder whose configuration is initialized with System Properties and $HOME/.td/td.conf values.
     * Precedence of properties is the following order:
     * <ol>
     * <li>System Properties</li>
     * <li>$HOME/.td/td.conf values</li>
     * </ol>
     *
     * @return
     */
    public TDClientBuilder(boolean loadTDConf)
    {
        // load the environnment variable for the api key
        String apiKeyEnv = System.getenv(ENV_TD_CLIENT_APIKEY);
        if (apiKeyEnv != null) {
            setApiKey(apiKeyEnv);
        }

        // load system properties
        setProperties(System.getProperties());

        // We also read $HOME/.td/td.conf file for apikey, user, and password values
        if (loadTDConf) {
            setProperties(getTDConfProperties());
        }
    }

    /**
     * Override the TDClient configuration with the give Properties
     *
     * @param p
     * @return
     */
    public TDClientBuilder setProperties(Properties p)
    {
        this.endpoint = getConfigProperty(p, TD_CLIENT_API_ENDPOINT).or(endpoint);
        this.port = getConfigPropertyInt(p, TD_CLIENT_API_PORT).or(port);
        if (p.containsKey(TD_CLIENT_USESSL)) {
            setUseSSL(Boolean.parseBoolean(p.getProperty(TD_CLIENT_USESSL)));
        }
        this.apiKey = getConfigProperty(p, TD_CLIENT_APIKEY)
                .or(getConfigProperty(p, "apikey"))
                .or(apiKey);
        this.user = getConfigProperty(p, TD_CLIENT_USER)
                .or(getConfigProperty(p, "user"))
                .or(user);
        this.password = getConfigProperty(p, TD_CLIENT_PASSOWRD)
                .or(getConfigProperty(p, "password"))
                .or(password);

        // proxy
        boolean hasProxy = false;
        ProxyConfig.ProxyConfigBuilder proxyConfig;
        if (proxy.isPresent()) {
            hasProxy = true;
            proxyConfig = new ProxyConfig.ProxyConfigBuilder(proxy.get());
        }
        else {
            proxyConfig = new ProxyConfig.ProxyConfigBuilder();
        }
        Optional<String> proxyHost = getConfigProperty(p, TD_CLIENT_PROXY_HOST);
        Optional<Integer> proxyPort = getConfigPropertyInt(p, TD_CLIENT_PROXY_PORT);
        Optional<String> proxyUseSSL = getConfigProperty(p, TD_CLIENT_PROXY_USESSL);
        Optional<String> proxyUser = getConfigProperty(p, TD_CLIENT_PROXY_USER);
        Optional<String> proxyPassword = getConfigProperty(p, TD_CLIENT_PROXY_PASSWORD);
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
        this.retryLimit = getConfigPropertyInt(p, TD_CLIENT_RETRY_LIMIT).or(retryLimit);
        this.retryInitialIntervalMillis = getConfigPropertyInt(p, TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS).or(retryInitialIntervalMillis);
        this.retryMaxIntervalMillis = getConfigPropertyInt(p, TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS).or(retryMaxIntervalMillis);
        this.retryMultiplier = getConfigPropertyDouble(p, TD_CLIENT_RETRY_MULTIPLIER).or(retryMultiplier);
        this.connectTimeoutMillis = getConfigPropertyInt(p, TD_CLIENT_CONNECT_TIMEOUT_MILLIS).or(connectTimeoutMillis);
        this.idleTimeoutMillis = getConfigPropertyInt(p, TD_CLIENT_IDLE_TIMEOUT_MILLIS).or(idleTimeoutMillis);
        this.connectionPoolSize = getConfigPropertyInt(p, TD_CLIENT_CONNECTION_POOL_SIZE).or(connectionPoolSize);
        return this;
    }

    public TDClientBuilder setEndpoint(String endpoint)
    {
        this.endpoint = Optional.of(endpoint);
        return this;
    }

    public TDClientBuilder setPort(int port)
    {
        this.port = Optional.of(port);
        return this;
    }

    public TDClientBuilder setUseSSL(boolean useSSL)
    {
        this.useSSL = useSSL;
        return this;
    }

    public TDClientBuilder setApiKey(String apiKey)
    {
        this.apiKey = Optional.of(apiKey);
        return this;
    }

    public TDClientBuilder setUser(String user)
    {
        this.user = Optional.of(user);
        return this;
    }

    public TDClientBuilder setPassword(String password)
    {
        this.password = Optional.of(password);
        return this;
    }

    public TDClientBuilder setProxy(ProxyConfig proxyConfig)
    {
        this.proxy = Optional.of(proxyConfig);
        return this;
    }

    public TDClientBuilder setRetryLimit(int retryLimit)
    {
        this.retryLimit = retryLimit;
        return this;
    }

    public TDClientBuilder setRetryInitialIntervalMillis(int retryInitialIntervalMillis)
    {
        this.retryInitialIntervalMillis = retryInitialIntervalMillis;
        return this;
    }

    public TDClientBuilder setRetryMaxIntervalMillis(int retryMaxIntervalMillis)
    {
        this.retryMaxIntervalMillis = retryMaxIntervalMillis;
        return this;
    }

    public void setRetryMultiplier(double retryMultiplier)
    {
        this.retryMultiplier = retryMultiplier;
    }

    public TDClientBuilder setConnectTimeoutMillis(int connectTimeoutMillis)
    {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public TDClientBuilder setIdleTimeoutMillis(int idleTimeoutMillis)
    {
        this.idleTimeoutMillis = idleTimeoutMillis;
        return this;
    }

    public TDClientBuilder setConnectionPoolSize(int connectionPoolSize)
    {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    public TDClient build()
    {
        return new TDClient(new TDClientConfig(
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
        ));
    }
}
