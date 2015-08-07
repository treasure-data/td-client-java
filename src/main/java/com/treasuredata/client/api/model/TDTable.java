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
package com.treasuredata.client.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;

import java.util.List;

public class TDTable
{
    private final String id;
    private final String name;
    private final TDTableType type;
    private final List<TDColumn> columns;
    private final long rowCount;
    private final long estimatedStorageSize;
    private final String lastLogTimeStamp;
    private final String expireDays;
    private final String createdAt;
    private final String updatedAt;

    @JsonCreator
    public TDTable(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("type") TDTableType type,
            @JsonProperty("schema") String schema,
            @JsonProperty("count") long rowCount,
            @JsonProperty("estimated_storage_size") long estimatedStroageSize,
            @JsonProperty("last_log_timestamp") String lastLogTimeStamp,
            @JsonProperty("expire_days") String expireDays,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt
    )
    {
        this.id = id;
        this.name = name;
        this.type = type;
        this.columns = TDColumn.parseTuple(schema);
        this.rowCount = rowCount;
        this.estimatedStorageSize = estimatedStroageSize;
        this.lastLogTimeStamp = lastLogTimeStamp;
        this.expireDays = expireDays;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TDTableType getType()
    {
        return type;
    }

    @JsonProperty
    public List<TDColumn> getColumns()
    {
        return columns;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getEstimatedStorageSize()
    {
        return estimatedStorageSize;
    }

    public String getLastLogTimeStamp()
    {
        return lastLogTimeStamp;
    }

    public String getExpireDays()
    {
        return expireDays;
    }

    public String getCreatedAt()
    {
        return createdAt;
    }

    public String getUpdatedAt()
    {
        return updatedAt;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TDTable other = (TDTable) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.columns, other.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type, columns);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", name, Joiner.on(", ").join(columns));
    }
}
