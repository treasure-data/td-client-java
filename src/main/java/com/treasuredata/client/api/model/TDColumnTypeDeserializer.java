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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

import java.io.IOException;

public class TDColumnTypeDeserializer
        extends JsonDeserializer<TDColumnType>
{
    @Override
    public TDColumnType deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException
    {
        if (jp.nextToken() != JsonToken.VALUE_STRING) {
            //throw new JsonMappingException("Unexpected JSON element to deserialize TDColumnType");
            throw new RuntimeJsonMappingException("Unexpected JSON element to deserialize TDColumnType");
        }
        String str = jp.getText();
        return parseColumnType(str);
    }

    public static TDColumnType parseColumnType(String str)
    {
        Parser p = new Parser(str);
        TDColumnType type = parseColumnTypeRecursive(p);
        if (!p.eof()) {
            throw new IllegalArgumentException("Cannot parse type: EOF expected: " + str);
        }
        return type;
    }

    private static TDColumnType parseColumnTypeRecursive(Parser p)
    {
        if (p.scan("string")) {
            return TDPrimitiveColumnType.STRING;
        }
        else if (p.scan("int")) {
            return TDPrimitiveColumnType.INT;
        }
        else if (p.scan("long")) {
            return TDPrimitiveColumnType.LONG;
        }
        else if (p.scan("double")) {
            return TDPrimitiveColumnType.DOUBLE;
        }
        else if (p.scan("float")) {
            return TDPrimitiveColumnType.FLOAT;
        }
        else if (p.scan("array")) {
            if (!p.scan("<")) {
                throw new IllegalArgumentException("Cannot parse type: expected '<' for array type: " + p.getString());
            }
            TDColumnType elementType = parseColumnTypeRecursive(p);
            if (!p.scan(">")) {
                throw new IllegalArgumentException("Cannot parse type: expected '>' for array type: " + p.getString());
            }
            return new TDArrayColumnType(elementType);
        }
        else if (p.scan("map")) {
            if (!p.scan("<")) {
                throw new IllegalArgumentException("Cannot parse type: expected '<' for map type: " + p.getString());
            }
            TDColumnType keyType = parseColumnTypeRecursive(p);
            if (!p.scan(",")) {
                throw new IllegalArgumentException("Cannot parse type: expected ',' for map type: " + p.getString());
            }
            TDColumnType valueType = parseColumnTypeRecursive(p);
            if (!p.scan(">")) {
                throw new IllegalArgumentException("Cannot parse type: expected '>' for map type: " + p.getString());
            }
            return new TDMapColumnType(keyType, valueType);
        }
        else {
            throw new IllegalArgumentException("Cannot parse type: " + p.getString());
        }
    }

    private static class Parser
    {
        private final String string;
        private int offset;

        public Parser(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }

        public boolean scan(String s)
        {
            skipSpaces();
            if (string.startsWith(s, offset)) {
                offset += s.length();
                return true;
            }
            return false;
        }

        public boolean eof()
        {
            skipSpaces();
            return string.length() <= offset;
        }

        private void skipSpaces()
        {
            while (string.startsWith(" ", offset)) {
                offset++;
            }
        }
    }
}
