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

import com.treasure_data.model.Table;
import com.treasure_data.model.TableSpecifyRequest;

public class RenameTableRequest extends TableSpecifyRequest<Table> {

    private String databaseName;
    private String origTableName;
    private String newTableName;
    private boolean overwrite = false;

    public RenameTableRequest(String databaseName, String origTableName, String newTableName) {
        this(databaseName, origTableName, newTableName, false);
    }

    public RenameTableRequest(String databaseName, String origTableName, String newTableName,
            boolean overwrite) {
        super(null);
        this.databaseName = databaseName;
        this.origTableName = origTableName;
        this.newTableName = newTableName;
        this.overwrite = overwrite;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getOrigTableName() {
        return origTableName;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public boolean getOverwrite() {
        return overwrite;
    }
}
