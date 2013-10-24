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

public enum DataType {
    STRING("string"),
    INT("int"),
    LONG("long"),
    DOUBLE("double"),
    FLOAT("float"),
    ARRAY("array");

    private String type;

    DataType(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }

    public static DataType fromString(String name) {
        return StringToDataType.get(name);
    }

    private static class StringToDataType {
        private static final Map<String, DataType> REVERSE_DICTIONARY;

        static {
            Map<String, DataType> map = new HashMap<String, DataType>();
            for (DataType elem : DataType.values()) {
                map.put(elem.type(), elem);
            }
            REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
        }

        static DataType get(String key) {
            return REVERSE_DICTIONARY.get(key);
        }
    }
}
