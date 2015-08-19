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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class TDTablePermission
{
    private boolean importable;
    private boolean queryable;

    public TDTablePermission(
            @JsonProperty("importable") boolean importable,
            @JsonProperty("queryable") boolean queryable)
    {
        this.importable = importable;
        this.queryable = queryable;
    }

    @JsonProperty("importable")
    public boolean isImportable()
    {
        return importable;
    }

    @JsonProperty("queryable")
    public boolean isQueryable()
    {
        return queryable;
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
        TDTablePermission other = (TDTablePermission) obj;
        return Objects.equal(this.importable, other.importable) &&
                Objects.equal(this.queryable, other.queryable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(importable, queryable);
    }
}
