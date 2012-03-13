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

import java.util.List;

import com.treasure_data.client.ClientAdaptor;
import com.treasure_data.client.ClientException;

public class Database extends AbstractModel {

    private ListTables tables = null;

    public Database(String name) {
        super(name);
    }

    public String getName() {
        return super.getName();
    }

    public void setListTables(ListTables tables) {
        this.tables = tables;
    }

    public List<Table> getTables(ClientAdaptor clientAdaptor) {
        if (tables == null) {
            try {
                clientAdaptor.listTables(new ListTablesRequest(this));
            } catch (ClientException e) {
                // ignore
            }
        }
        return tables.getList();
    }

    public boolean deleteTable(String tableName) {
        if (tables == null) {
            return true;
        } else {
            return tables.delete(tableName);
        }
    }
}
