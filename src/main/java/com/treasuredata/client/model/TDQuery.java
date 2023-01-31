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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringWriter;

/**
 *
 */
public class TDQuery
{
    private final String query;

    public TDQuery(String query)
    {
        this.query = query;
    }

    @JsonCreator
    public static TDQuery fromString(String s)
    {
        return new TDQuery(s);
    }

    @JsonCreator
    public static TDQuery fromObject(JsonNode value)
    {
        // embulk job have nested json object
        try (StringWriter s = new StringWriter()) {
            new ObjectMapper().writeValue(s, value);
            return new TDQuery(s.toString());
        }
        catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getQuery()
    {
        return query;
    }

    @Override
    public String toString()
    {
        return query;
    }
}
