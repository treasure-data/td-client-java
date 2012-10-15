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

import com.treasure_data.model.Table;
import com.treasure_data.model.TableSpecifyRequest;

public class SwapTableRequest extends TableSpecifyRequest<Table> {

    private String databaseName;

    private String tableName1;

    private String tableName2;

    public SwapTableRequest(String databaseName, String tableName1, String tableName2) {
        super(null);
        this.databaseName = databaseName;
        this.tableName1 = tableName1;
        this.tableName2 = tableName2;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName1() {
        return tableName1;
    }

    public String getTableName2() {
        return tableName2;
    }
}
