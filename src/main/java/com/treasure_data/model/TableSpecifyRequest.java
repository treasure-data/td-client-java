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

public class TableSpecifyRequest<T extends Table> extends AbstractRequest<T> {
    protected TableSpecifyRequest(T table) {
        super(table);
    }

    public Database getDatabase() {
        return getTable().getDatabase();
    }

    public T getTable() {
        return get();
    }

    protected void setDatabaseName(String name) {
        get().getDatabase().setName(name);
    }

    public String getDatabaseName() {
        return get().getDatabase().getName();
    }

    protected void setTableName(String name) {
        get().setName(name);
    }

    public String getTableName() {
        return get().getName();
    }

}
