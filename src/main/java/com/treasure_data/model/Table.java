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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Table extends AbstractModel {
    public static enum Type {
        LOG("log"), ITEM("item");

        private String type;

        Type(String type) {
            this.type = type;
        }

        public String type() {
            return type;
        }

        public static Type fromString(String type) {
            return StringToType.get(type);
        }

        private static class StringToType {
            private static final Map<String, Type> REVERSE_DICTIONARY;

            static {
                Map<String, Type> map = new HashMap<String, Type>();
                for (Type e : Type.values()) {
                    map.put(e.type(), e);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static Type get(String type) {
                return REVERSE_DICTIONARY.get(type);
            }
        }
    }

    private Database database;

    private Type type;

    public Table(Database database, String name) {
        this(database, name, Table.Type.LOG);
    }

    public Table(Database database, String name, Type type) {
        super(name);
        this.database = database;
        this.type = type;
    }

    public Database getDatabase() {
        return database;
    }

    public String getName() {
        return super.getName();
    }

    public Type getType() {
        return type;
    }

}
