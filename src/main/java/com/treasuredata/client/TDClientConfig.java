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
import com.google.common.collect.ImmutableSet;
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

    public static final ImmutableSet<String> knownProperties = ImmutableSet.<String>builder()
            .add("apikey")
            .add("user")
            .add("password")
            .add(TD_CLIENT_APIKEY)
            .add(TD_CLIENT_USER)
            .add(TD_CLIENT_PASSOWRD)
            .add(TD_CLIENT_USESSL)
            .add(TD_CLIENT_API_ENDPOINT)
            .add(TD_CLIENT_API_PORT)
            .add(TD_CLIENT_RETRY_LIMIT)
            .add(TD_CLIENT_RETRY_INITIAL_INTERVAL_MILLIS)
            .add(TD_CLIENT_RETRY_MAX_INTERVAL_MILLIS)
            .add(TD_CLIENT_RETRY_MULTIPLIER)
            .add(TD_CLIENT_CONNECT_TIMEOUT_MILLIS)
            .add(TD_CLIENT_IDLE_TIMEOUT_MILLIS)
            .add(TD_CLIENT_CONNECTION_POOL_SIZE)
            .add(TD_CLIENT_PROXY_HOST)
            .add(TD_CLIENT_PROXY_PORT)
            .add(TD_CLIENT_PROXY_USER)
            .add(TD_CLIENT_PROXY_PASSWORD)
            .build();

    /**
     * endpoint URL (e.g., api.treasuredata.com, ybi.jp-east.idcfcloud.com)
     */
    public final String endpoint;
    public final Optional<Integer> port;
    public final Optional<String> apiKey;
    public final Optional<String> user;
    public final Optional<String> password;
    public final Optional<ProxyConfig> proxy;
    public final boolean useSSL;
    public final int retryLimit;
    public final int retryInitialIntervalMillis;
    public final int retryMaxIntervalMillis;
    public final double retryMultiplier;
    public final int connectTimeoutMillis;
    public final int idleTimeoutMillis;
    public final int connectionPoolSize;

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

    private static Logger logger = LoggerFactory.getLogger(TDClientConfig.class);
    private static Properties tdConf;

    /**
     * Get the default TDClientConfig by reading $HOME/.td/td.conf file.
     *
     * @return
     * @throws IOException
     * @throws TDClientException
     */
    static Properties getTDConfProperties()
    {
        if (tdConf == null) {
            tdConf = readTDConf();
        }
        return tdConf;
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
}
