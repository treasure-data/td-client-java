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
    private static Logger logger = LoggerFactory.getLogger(TDClientConfig.class);
    public static final String TD_CLIENT_APIKEY = "TD_API_KEY";
    private final String endpoint;
    private final String apiKey;
    private final String internalKey;
    private final String internalKeyVersion;
    private final int retryLimit = 5;
    private final int retryInitialWaitMillis = 1000;
    private final int retryWaitMillis = 2000;

    public static <V> V checkNotNull(V v, String message)
            throws TDClientException
    {
        if (v == null) {
            throw new TDClientException(ErrorCode.INVALID_CONFIGURATION, message);
        }
        return v;
    }

    public TDClientConfig(String apiKey)
            throws TDClientException
    {
        this("api.treasuredata.com", apiKey, null, null);
    }

    public TDClientConfig(String endpoint, String apiKey, String internalKey, String internalKeyVersion)
            throws TDClientException
    {
        this.endpoint = checkNotNull(endpoint, "API endpoint is null");
        this.apiKey = checkNotNull(apiKey, "apikey is null. Check you $HOME/.td/td.conf file");
        this.internalKey = internalKey;
        this.internalKeyVersion = internalKeyVersion;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public String getApiKey()
    {
        return apiKey;
    }

    public String getInternalKey()
    {
        return internalKey;
    }

    public String getInternalKeyVersion()
    {
        return internalKeyVersion;
    }

    public int getRetryLimit()
    {
        return retryLimit;
    }

    public int getRetryInitialWaitMillis()
    {
        return retryInitialWaitMillis;
    }

    public int getRetryWaitMillis()
    {
        return retryWaitMillis;
    }

    public static TDClientConfig currentConfig()
            throws IOException, TDClientException
    {
        Properties p = readTDConf();
        String apiKey = firstNonNull(p.getProperty("apikey"), System.getenv().get(TD_CLIENT_APIKEY));
        return new TDClientConfig(apiKey);
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
}
