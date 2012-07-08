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
package com.treasure_data.model.bulkimport;

import java.util.List;

import com.treasure_data.model.AbstractModel;

public class Session extends AbstractModel {

    private String databaseName;

    private String tableName;

    private List<String> files;

    public Session(String name, String databaseName, String tableName) {
        super(name);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getUploadedFiles() {
        return files;
    }

    @Override
    public String toString() {
        return String.format("Session{name=%s, db=%s, tbl=%s}",
                getName(), getDatabaseName(), getTableName());
    }
}
