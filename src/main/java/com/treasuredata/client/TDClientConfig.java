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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
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
    public static enum Type
    {
        APIKEY("td.client.apikey", "API key to access Treasure Data."),
        USER("td.client.user", "Account e-mail address (unnecessary if apikey is set)"),
        PASSOWRD("td.client.password", "Account apssword (unnecessary if apikey is set"),
        USESSL("td.client.usessl", "Use SSL encryption"),
        API_ENDPOINT("td.client.endpoint", "TD API end point (e.g., api.treasuredata.com"),
        API_PORT("td.client.port", "TD API port number"),
        RETRY_LIMIT("td.client.retry.limit", "The maximum nubmer of API request retry"),
        RETRY_INITIAL_INTERVAL_MILLIS("td.client.retry.initial-interval", "backoff retry interval = (interval) * (multiplier) ^ (retry count)"),
        RETRY_MAX_INTERVAL_MILLIS("td.client.retry.max-interval", "max retry interval"),
        RETRY_MULTIPLIER("td.client.retry.multiplier", "retry interval multiplier"),
        CONNECT_TIMEOUT_MILLIS("td.client.connect-timeout", "connection timeout before reaching the API"),
        IDLE_TIMEOUT_MILLIS("td.client.idle-timeout", "idle connection timeout when no data is coming from API"),
        CONNECTION_POOL_SIZE("td.client.connection-pool-size", "connection pool size"),
        PROXY_HOST("td.client.proxy.host", "Proxy host (e.g., myproxy.com)"),
        PROXY_PORT("td.client.proxy.port", "Proxy port number"),
        PROXY_USER("td.client.proxy.user", "Proxy user name"),
        PROXY_PASSWORD("td.client.proxy.password", "Proxy paassword"),
        PROXY_USESSL("td.client.proxy.usessl", "Use SSL for proxy");

        public final String key;
        public final String description;

        Type(String key, String description)
        {
            this.key = key;
            this.description = description;
        }
    }

    public static List<Type> knownProperties()
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (Type t : Type.values()) {
            builder.add(t);
        }
        return builder.build();
    }

    /**
     * endpoint URL (e.g., api.treasuredata.com, api.ybi.idcfcloud.net)
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

    private static <V> void saveProperty(Properties p, Type config, V value)
    {
        if (value == null) {
            return;
        }

        if (value instanceof Optional) {
            Optional<?> opt = (Optional<?>) value;
            if (opt.isPresent()) {
                Object v = opt.get();
                saveProperty(p, config, v);
            }
        }
        else {
            p.setProperty(config.key, value.toString());
        }
    }

    /**
     * Output this configuration as a Properties object
     *
     * @return
     */
    public Properties toProperties()
    {
        Properties p = new Properties();
        saveProperty(p, Type.API_ENDPOINT, endpoint);
        saveProperty(p, Type.API_PORT, port);
        saveProperty(p, Type.USESSL, useSSL);
        saveProperty(p, Type.APIKEY, apiKey);
        saveProperty(p, Type.USER, user);
        saveProperty(p, Type.PASSOWRD, password);
        if (proxy.isPresent()) {
            ProxyConfig pc = proxy.get();
            saveProperty(p, Type.PROXY_HOST, pc.getHost());
            saveProperty(p, Type.PROXY_PORT, pc.getPort());
            saveProperty(p, Type.PROXY_USER, pc.getUser());
            saveProperty(p, Type.PROXY_PASSWORD, pc.getPassword());
            saveProperty(p, Type.PROXY_USESSL, pc.useSSL());
        }
        saveProperty(p, Type.RETRY_LIMIT, retryLimit);
        saveProperty(p, Type.RETRY_INITIAL_INTERVAL_MILLIS, retryInitialIntervalMillis);
        saveProperty(p, Type.RETRY_MAX_INTERVAL_MILLIS, retryMaxIntervalMillis);
        saveProperty(p, Type.RETRY_MULTIPLIER, retryMultiplier);
        saveProperty(p, Type.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);
        saveProperty(p, Type.CONNECTION_POOL_SIZE, connectionPoolSize);
        return p;
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

            // Parse endpoint url if necessary
            if (p.containsKey("endpoint")) {
                parseEndpoint(p);
            }
            return p;
        }
        catch (IOException e) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_CONFIGURATION, String.format("Failed to read config file: %s", file), e);
        }
    }

    private static void parseEndpoint(Properties p)
    {
        String endpoint = p.getProperty("endpoint");
        URI uri;
        try {
            uri = new URI(endpoint);
        }
        catch (URISyntaxException ignore) {
            // The endpoint was not a URL, let it be as is
            return;
        }

        int defaultPort;
        boolean useSsl;

        switch (uri.getScheme()) {
            case "http":
                defaultPort = 80;
                useSsl = false;
                break;
            case "https":
                defaultPort = 443;
                useSsl = true;
                break;
            default:
                throw new IllegalArgumentException("Invalid endpoint scheme: " + uri.getScheme());
        }

        p.setProperty("endpoint", uri.getHost());

        if (!p.containsKey("port")) {
            int port = (uri.getPort() == -1) ? defaultPort : uri.getPort();
            p.setProperty("port", Integer.toString(port));
        }

        if (!p.contains("usessl")) {
            p.setProperty("usessl", Boolean.toString(useSsl));
        }
    }
}
