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

public class SubmitJobRequest extends AbstractRequest<Job> {

    private Database database;

    private String query;

    private String resultTableName;

    private boolean wait;

    public SubmitJobRequest(Database database, String query, String resultTableName, boolean wait) {
        super(null);
        this.database = database;
        this.query = query;
        this.resultTableName = resultTableName;
        this.wait = wait;
    }

    public Database getDatabase() {
        return database;
    }

    public String getQuery() {
        return query;
    }

    public String getResultTableName() {
        return resultTableName;
    }

    public boolean isWait() {
        return wait;
    }
}
