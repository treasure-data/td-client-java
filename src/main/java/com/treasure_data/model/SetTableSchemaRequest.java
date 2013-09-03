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

import org.json.simple.JSONValue;

public class SetTableSchemaRequest extends AbstractRequest<TableSchema> {

    public SetTableSchemaRequest(TableSchema schema) {
        super(schema);
    }

    public TableSchema getTableSchema() {
        return get();
    }

    public String getDatabaseName() {
        return get().getDatabase().getName();
    }

    public String getTableName() {
        return get().getTable().getName();
    }

    public String getJSONString() {
        List<List<String>> ret = new ArrayList<List<String>>();

        List<TableSchema.Pair> pairs = get().getPairsOfColsAndTypes();
        for (TableSchema.Pair p : pairs) {
            List<String> pair = new ArrayList<String>();
            pair.add(p.getColumnName());
            pair.add(p.getType().toString());
            ret.add(pair);
        }

        return JSONValue.toJSONString(ret);
    }
}
