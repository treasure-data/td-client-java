/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TDImportResult
{
    private final String databaseName;
    private final String tableName;
    private final String uniqueId;
    private final long elapsedTime;
    private final String md5Hex;

    @JsonCreator
    public TDImportResult(
            @JsonProperty("database") String databaseName,
            @JsonProperty("table") String tableName,
            @JsonProperty("elapsed_time") long elapsedTime,
            @JsonProperty("unique_id") String uniqueId,
            @JsonProperty("md5_hex") String md5Hex)
    {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.elapsedTime = elapsedTime;
        this.uniqueId = uniqueId;
        this.md5Hex = md5Hex;
    }

    @JsonProperty("database")
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty("table")
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty("unique_id")
    public String getUniqueId()
    {
        return uniqueId;
    }

    @JsonProperty("elapsed_time")
    public long getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty("md5_hex")
    public String getMd5Hex()
    {
        return md5Hex;
    }
}
