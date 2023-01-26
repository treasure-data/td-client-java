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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.google.common.base.Objects;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class TDColumn implements Serializable
{
    static final byte[] LOG_TABLE_PUSHDOWN_KEY = "time".getBytes(UTF_8);

    // The column SQL alias.
    private final String name;
    private final TDColumnType type;
    // The physical column name in partition files
    private final byte[] key;

    public TDColumn(String name, TDColumnType type)
    {
        this(name, type, name.getBytes(StandardCharsets.UTF_8));
    }

    @JsonCreator
    public TDColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("key") String key)
    {
        this(name, TDColumnType.parseColumnType(type), key.getBytes(UTF_8));
    }

    public TDColumn(String name, TDColumnType type, byte[] key)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.key = Arrays.copyOf(requireNonNull(key, "key is null"), key.length);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TDColumnType getType()
    {
        return type;
    }

    @JsonIgnore
    public byte[] getKey()
    {
        return Arrays.copyOf(key, key.length);
    }

    @JsonProperty("key")
    public String getKeyString()
    {
        return new String(key, StandardCharsets.UTF_8);
    }

    private static JSONArray castToArray(Object obj)
    {
        if (obj instanceof JSONArray) {
            return (JSONArray) obj;
        }
        else {
            throw new RuntimeJsonMappingException("Not an json array: " + obj);
        }
    }

    public static List<TDColumn> parseTuple(String jsonStr)
    {
        // unescape json quotation
        try {
            String unescaped = jsonStr.replaceAll("\\\"", "\"");
            JSONArray arr = castToArray(new JSONParser().parse(unescaped));
            List<TDColumn> columnList = new ArrayList<TDColumn>(arr.size());
            for (Object e : arr) {
                JSONArray columnNameAndType = castToArray(e);
                String[] s = new String[columnNameAndType.size()];
                for (int i = 0; i < columnNameAndType.size(); ++i) {
                    s[i] = columnNameAndType.get(i).toString();
                }
                columnList.add(parseTuple(s));
            }
            return columnList;
        }
        catch (ParseException e) {
            LoggerFactory.getLogger(TDColumn.class).error("Failed to parse json string", e);
            return new ArrayList<TDColumn>(0);
        }
    }

    public static TDColumn parseTuple(String[] tuple)
    {
        // TODO encode key in some ways
        if (tuple != null) {
            if (tuple.length == 2) {
                // [ key, type ]
                return new TDColumn(
                        tuple[0],
                        TDColumnType.parseColumnType(tuple[1]),
                        tuple[0].getBytes(StandardCharsets.UTF_8));
            }
            else if (tuple.length == 3) {
                // [ key, type, name ]
                return new TDColumn(
                        tuple[2],
                        TDColumnType.parseColumnType(tuple[1]),
                        tuple[0].getBytes(StandardCharsets.UTF_8));
            }
        }
        throw new RuntimeJsonMappingException("Unexpected string tuple to deserialize TDColumn");
    }

    @JsonIgnore
    public boolean isPartitionKey()
    {
        return Arrays.equals(LOG_TABLE_PUSHDOWN_KEY, getKey());
    }

    @JsonIgnore
    public String[] getTuple()
    {
        return new String[] {getKeyString(), type.toString(), name};
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TDColumn other = (TDColumn) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(type, other.type) &&
                Arrays.equals(key, other.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type, Arrays.hashCode(key));
    }

    @Override
    public String toString()
    {
        return String.format("%s:%s", name, type.toString());
    }
}
