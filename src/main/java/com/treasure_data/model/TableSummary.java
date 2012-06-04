//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
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

public class TableSummary extends Table {

    private long count;

    private String schema;

    private String createdAt;

    private String updatedAt;

    public TableSummary(Database database, String name,
            long count, String schema, String createdAt, String updatedAt) {
        this(database, name, Table.Type.LOG,
                count, schema, createdAt, updatedAt);
    }

    public TableSummary(Database database, String name, Table.Type type,
            long count, String schema, String createdAt, String updatedAt) {
        super(database, name, type);
        this.count = count;
        this.schema = schema;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public long getCount() {
        return count;
    }

    public String getSchema() {
        return schema;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }
}
