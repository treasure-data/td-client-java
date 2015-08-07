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
package com.treasuredata.client.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.google.common.base.Objects;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TDColumn
{
    private static Logger logger = LoggerFactory.getLogger(TDColumn.class);
    private String name;
    private TDColumnType type;
    private byte[] key;

    public TDColumn(String name, TDColumnType type, byte[] key)
    {
        this.name = name;
        this.type = type;
        this.key = key;
    }

    public String getName()
    {
        return name;
    }

    public TDColumnType getType()
    {
        return type;
    }

    public byte[] getKey()
    {
        return key;
    }

    private static List<TDColumn> emptyList = new ArrayList<TDColumn>(0);

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
            logger.error("Failed to parse json string", e);
            return emptyList;
        }
    }

    @JsonCreator
    public static TDColumn parseTuple(String[] tuple)
    {
        // TODO encode key in some ways
        if (tuple != null) {
            if (tuple.length == 2) {
                return new TDColumn(
                        tuple[0],
                        TDColumnTypeDeserializer.parseColumnType(tuple[1]),
                        tuple[0].getBytes());
            }
            else if (tuple.length == 3) {
                return new TDColumn(
                        tuple[0],
                        TDColumnTypeDeserializer.parseColumnType(tuple[1]),
                        tuple[2].getBytes());
            }
        }
        throw new RuntimeJsonMappingException("Unexpected string tuple to deserialize TDColumn");
    }

    @JsonValue
    public String[] getTuple()
    {
        return new String[] {name, type.toString(), new String(key)};
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
                Objects.equal(key, other.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type, key);
    }

    @Override
    public String toString()
    {
        return String.format("%s:%s", name, type.toString());
    }
}
