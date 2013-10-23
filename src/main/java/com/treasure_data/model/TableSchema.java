//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.model;

import java.util.ArrayList;
import java.util.List;

public class TableSchema extends AbstractModel {
    public static interface Type {
        String toString();
    }

    public static interface ContainerType extends Type {
        String getContainerType();
    }

    public static class ArrayType implements ContainerType {
        private Type elementType;

        private ArrayType(Type elementType) {
            this.elementType = elementType;
        }

        public String getContainerType() {
            return "array";
        }

        @Override
        public String toString() {
            return getContainerType() + "<" + elementType.toString() + ">";
        }
    }

    public static class PrimitiveType implements Type {
        private static final PrimitiveType STRING = new PrimitiveType("string");
        private static final PrimitiveType INT = new PrimitiveType("int");
        private static final PrimitiveType LONG = new PrimitiveType("long");
        private static final PrimitiveType DOUBLE = new PrimitiveType("double");
        private static final PrimitiveType FLOAT = new PrimitiveType("float");

        private String type;

        private PrimitiveType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return getType();
        }
    }

    public static class Pair {
        String col;
        Type type;

        Pair(String col, Type type) {
            this.col = col;
            this.type = type;
        }

        public String getColumnName() {
            return col;
        }

        public Type getType() {
            return type;
        }

        @Override
        public String toString() {
            return col + ":" + type;
        }
    }

    public static List<Pair> parsePairs(List<String> pairList) {
        if (pairList == null || pairList.isEmpty()) {
            return new ArrayList<Pair>();
        }

        List<Pair> pairs = new ArrayList<Pair>();
        for (String pair : pairList) {
            pairs.add(parsePair(pair));
        }
        return pairs;
    }

    public static Pair parsePair(String pairString) {
        String[] pair = pairString.split(":");

        if (pair.length != 2) {
            throw new IllegalArgumentException(""); // TODO
        }

        return new Pair(pair[0], parseType(pair[1]));
    }

    private static Type parseType(String typeString) {
        if (PrimitiveType.STRING.getType().equals(typeString)) {
            return PrimitiveType.STRING;
        } else if (PrimitiveType.INT.getType().equals(typeString)) {
            return PrimitiveType.INT;
        } else if (PrimitiveType.LONG.getType().equals(typeString)) {
            return PrimitiveType.LONG;
        } else if (PrimitiveType.DOUBLE.getType().equals(typeString)) {
            return PrimitiveType.DOUBLE;
        } else if (PrimitiveType.FLOAT.getType().equals(typeString)) {
            return PrimitiveType.FLOAT;
        } else if (typeString.startsWith("array<")) {
            // TODO refine the parser more
            typeString = typeString.substring(0, "array<".length());
            typeString = typeString.substring(0, typeString.length() - 1);
            return new ArrayType(parseType(typeString));
        } else {
            throw new IllegalArgumentException(""); // TODO
        }
    }

    protected Table table;
    protected List<Pair> pairs;

    public TableSchema(Table table) {
        this(table, null);
    }

    public TableSchema(Table table, List<String> pairsOfColsAndTypes) {
        super(table.getName());
        this.table = table;
        setPairsOfColsAndTypes(pairsOfColsAndTypes);
    }

    public void setPairs(List<Pair> pairs) {
        this.pairs = pairs;
    }

    public void setPairsOfColsAndTypes(List<String> pairsOfColsAndTypes) {
        if (pairsOfColsAndTypes == null || pairsOfColsAndTypes.isEmpty()) {
            this.pairs = new ArrayList<Pair>();
        }

        this.pairs = parsePairs(pairsOfColsAndTypes);
    }

    public Database getDatabase() {
        return table.getDatabase();
    }

    public Table getTable() {
        return table;
    }

    public String getName() {
        return table.getName();
    }

    public List<Pair> getPairsOfColsAndTypes() {
        return pairs;
    }
}
