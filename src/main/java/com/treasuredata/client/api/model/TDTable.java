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
import com.google.common.base.Objects;

import java.util.List;

public class TDTable
{
    private String name;
    private TDTableType type;
    private List<TDColumn> columns;

    @JsonCreator
    public TDTable(
            @JsonProperty("name") String name,
            @JsonProperty("type") TDTableType type,
            @JsonProperty("columns") List<TDColumn> columns)
    {
        this.name = name;
        this.type = type;
        this.columns = columns;
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
}
