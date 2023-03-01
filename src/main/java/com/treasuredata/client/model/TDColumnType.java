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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.treasuredata.client.model.impl.TDColumnTypeDeserializer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonDeserialize(using = TDColumnTypeDeserializer.class)
public class TDColumnType implements Serializable
{
    public static final TDColumnType INT = new TDColumnType(TDTypeName.INT, Collections.emptyList());
    public static final TDColumnType LONG = new TDColumnType(TDTypeName.LONG, Collections.emptyList());
    public static final TDColumnType FLOAT = new TDColumnType(TDTypeName.FLOAT, Collections.emptyList());
    public static final TDColumnType DOUBLE = new TDColumnType(TDTypeName.DOUBLE, Collections.emptyList());
    public static final TDColumnType STRING = new TDColumnType(TDTypeName.STRING, Collections.emptyList());

    public static final List<TDColumnType> primitiveTypes = Arrays.asList(INT, LONG, FLOAT, DOUBLE, STRING);

    public static TDColumnType newArrayType(TDColumnType elementType)
    {
        return new TDColumnType(TDTypeName.ARRAY, Collections.singletonList(elementType));
    }

    public static TDColumnType newMapType(TDColumnType keyType, TDColumnType valueType)
    {
        return new TDColumnType(TDTypeName.MAP, Arrays.asList(keyType, valueType));
    }

    private final TDTypeName typeName;
    private final List<TDColumnType> elementTypes;

    private TDColumnType(TDTypeName typeName, List<TDColumnType> elementTypes)
    {
        this.typeName = typeName;
        this.elementTypes = elementTypes;
    }

    public TDTypeName getTypeName()
    {
        return typeName;
    }

    public boolean isPrimitive()
    {
        return elementTypes.size() == 0;
    }

    public boolean isArrayType()
    {
        return typeName == TDTypeName.ARRAY;
    }

    public boolean isMapType()
    {
        return typeName == TDTypeName.MAP;
    }

    public TDColumnType getArrayElementType()
    {
        if (!isArrayType()) {
            throw new UnsupportedOperationException("getArrayElementType is not supported for " + this);
        }
        return elementTypes.get(0);
    }

    public TDColumnType getMapKeyType()
    {
        if (!isMapType()) {
            throw new UnsupportedOperationException("getmapKeyType is not supported for " + this);
        }
        return elementTypes.get(0);
    }

    public TDColumnType getMapValueType()
    {
        if (!isMapType()) {
            throw new UnsupportedOperationException("getMapValueType is not supported for " + this);
        }
        return elementTypes.get(1);
    }

    @JsonValue
    public String toString()
    {
        if (isArrayType()) {
            return String.format("array<%s>", getArrayElementType());
        }
        else if (isMapType()) {
            return String.format("map<%s,%s>", getMapKeyType(), getMapValueType());
        }
        else {
            return typeName.toString();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TDColumnType that = (TDColumnType) o;
        if (!typeName.equals(that.typeName)) {
            return false;
        }
        return Objects.equals(elementTypes, that.elementTypes);
    }

    @Override
    public int hashCode()
    {
        int result = typeName != null ? typeName.hashCode() : 0;
        result = 31 * result + (elementTypes != null ? elementTypes.hashCode() : 0);
        return result;
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
            return STRING;
        }
        else if (p.scan("int")) {
            return INT;
        }
        else if (p.scan("long")) {
            return LONG;
        }
        else if (p.scan("double")) {
            return DOUBLE;
        }
        else if (p.scan("float")) {
            return FLOAT;
        }
        else if (p.scan("array")) {
            if (!p.scan("<")) {
                throw new IllegalArgumentException("Cannot parse type: expected '<' for array type: " + p.getString());
            }
            TDColumnType elementType = parseColumnTypeRecursive(p);
            if (!p.scan(">")) {
                throw new IllegalArgumentException("Cannot parse type: expected '>' for array type: " + p.getString());
            }
            return newArrayType(elementType);
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
            return newMapType(keyType, valueType);
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
