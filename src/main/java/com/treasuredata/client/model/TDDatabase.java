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

import java.util.Objects;
import java.util.Optional;

@JsonCollectionRootName(value = "databases")
public class TDDatabase
{
    private final String id;
    private final String name;
    private final long count;
    private final long userId;
    private final String description;
    private final String createdAt;
    private final String updatedAt;
    private final Optional<String> organization;
    private final String permission;

    @JsonCreator
    public TDDatabase(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("user_id") long userId,
            @JsonProperty("description") String description,
            @JsonProperty("count") long count,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt,
            @JsonProperty("organization") Optional<String> organization,
            @JsonProperty("permission") String permission
    )
    {
        this.id = id;
        this.name = name;
        this.userId = userId;
        this.description = description;
        this.count = count;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.organization = organization;
        this.permission = permission;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public long getUserId()
    {
        return userId;
    }

    public String getDescription()
    {
        return description;
    }
    /**
     * Record count
     *
     * @return
     */
    @JsonProperty
    public long getCount()
    {
        return count;
    }

    @JsonProperty
    public String getCreatedAt()
    {
        return createdAt;
    }

    @JsonProperty
    public String getUpdatedAt()
    {
        return updatedAt;
    }

    @JsonProperty
    public Optional<String> getOrganization()
    {
        return organization;
    }

    @JsonProperty
    public String getPermission()
    {
        return permission;
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
        TDDatabase other = (TDDatabase) obj;
        return Objects.equals(this.name, other.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
