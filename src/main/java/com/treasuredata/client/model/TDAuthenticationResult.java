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
package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The result of calling /v3/authenticate
 */
public class TDAuthenticationResult
{
    private final String name;
    private final String apikey;

    @JsonCreator
    public TDAuthenticationResult(@JsonProperty("name") String name, @JsonProperty("apikey") String apikey)
    {
        this.name = name;
        this.apikey = apikey;
    }

    public String getName()
    {
        return name;
    }

    public String getApikey()
    {
        return apikey;
    }

    @Override
    public String toString()
    {
        return "TDAuthenticationResult{" +
                "name='" + name + '\'' +
                ", apikey='" + apikey + '\'' +
                '}';
    }
}
