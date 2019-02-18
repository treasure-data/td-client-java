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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

/**
 * Update request for saved queries. Use {@link TDSavedQuery#newUpdateRequestBuilder()} to build this object.
 */
public class TDSavedQueryUpdateRequest
{
    private final Optional<String> name;
    private final Optional<String> cron;
    private final Optional<TDJob.Type> type;
    private final Optional<String> query;
    private final Optional<String> timezone;
    private final Optional<Long> delay;
    private final Optional<String> database;
    private final Optional<Integer> priority;
    private final Optional<Integer> retryLimit;
    private final Optional<String> result;
    private final Optional<TDJob.EngineVersion> engineVersion;

    @VisibleForTesting
    @JsonCreator
    TDSavedQueryUpdateRequest(
            @JsonProperty("name") Optional<String> name,
            @JsonProperty("cron") Optional<String> cron,
            @JsonProperty("type") Optional<TDJob.Type> type,
            @JsonProperty("query") Optional<String> query,
            @JsonProperty("timezone") Optional<String> timezone,
            @JsonProperty("delay") Optional<Long> delay,
            @JsonProperty("database") Optional<String> database,
            @JsonProperty("priority") Optional<Integer> priority,
            @JsonProperty("retry_limit") Optional<Integer> retryLimit,
            @JsonProperty("result") Optional<String> result,
            @JsonProperty("engine_version") Optional<TDJob.EngineVersion> engineVersion)
    {
        this.name = name;
        this.cron = cron;
        this.type = type;
        this.query = query;
        this.timezone = timezone;
        this.delay = delay;
        this.database = database;
        this.priority = priority;
        this.retryLimit = retryLimit;
        this.result = result;
        this.engineVersion = engineVersion;
    }

    @VisibleForTesting
    static ObjectMapper getObjectMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        // Configure object mapper to exclude Optional.absent values in the generated json string
        mapper.registerModule(new GuavaModule().configureAbsentsAsNulls(false));
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        return mapper;
    }

    public String toJson()
    {
        try {
            return getObjectMapper().writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            // This should not happen in general
            throw new IllegalStateException(String.format("Failed to produce json data of TDSavedQueryUpdateRequest: %s", this), e);
        }
    }

    /**
     * Apply this update to the given TDSaveQuery objects and create a new TDSaveQueryRequest object.
     *
     * @param base
     * @return
     */
    @VisibleForTesting
    public TDSaveQueryRequest merge(TDSavedQuery base)
    {
        return new TDSaveQueryRequest(
                name.or(base.getName()),
                cron.or(base.getCron()),
                type.or(base.getType()),
                query.or(base.getQuery()),
                timezone.or(base.getTimezone()),
                delay.or(base.getDelay()),
                database.or(base.getDatabase()),
                priority.or(base.getPriority()),
                retryLimit.or(base.getRetryLimit()),
                result.or(base.getResult()),
                engineVersion.isPresent() ? engineVersion.get() : base.getEngineVersion());
    }

    @JsonProperty
    public Optional<String> getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getCron()
    {
        return cron;
    }

    @JsonProperty
    public Optional<TDJob.Type> getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
    }

    @JsonProperty
    public Optional<String> getTimezone()
    {
        return timezone;
    }

    @JsonProperty
    public Optional<Long> getDelay()
    {
        return delay;
    }

    @JsonProperty
    public Optional<String> getDatabase()
    {
        return database;
    }

    @JsonProperty
    public Optional<Integer> getPriority()
    {
        return priority;
    }

    @JsonProperty("retry_limit")
    public Optional<Integer> getRetryLimit()
    {
        return retryLimit;
    }

    @JsonProperty
    public Optional<String> getResult()
    {
        return result;
    }

    @JsonProperty("engine_version")
    public Optional<TDJob.EngineVersion> getEngineVersion()
    {
        return engineVersion;
    }

    @Override
    public String toString()
    {
        return "TDSavedQueryUpdateRequest{" +
                "name=" + name +
                ", cron=" + cron +
                ", type=" + type +
                ", query=" + query +
                ", timezone=" + timezone +
                ", delay=" + delay +
                ", database=" + database +
                ", priority=" + priority +
                ", retryLimit=" + retryLimit +
                ", result=" + result +
                ", engineVersion=" + engineVersion +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TDSavedQueryUpdateRequest that = (TDSavedQueryUpdateRequest) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (cron != null ? !cron.equals(that.cron) : that.cron != null) {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }
        if (query != null ? !query.equals(that.query) : that.query != null) {
            return false;
        }
        if (timezone != null ? !timezone.equals(that.timezone) : that.timezone != null) {
            return false;
        }
        if (delay != null ? !delay.equals(that.delay) : that.delay != null) {
            return false;
        }
        if (database != null ? !database.equals(that.database) : that.database != null) {
            return false;
        }
        if (priority != null ? !priority.equals(that.priority) : that.priority != null) {
            return false;
        }
        if (retryLimit != null ? !retryLimit.equals(that.retryLimit) : that.retryLimit != null) {
            return false;
        }
        if (engineVersion != null ? !engineVersion.equals(that.engineVersion) : that.engineVersion != null) {
            return false;
        }
        return result != null ? result.equals(that.result) : that.result == null;
    }

    @Override
    public int hashCode()
    {
        int result1 = name != null ? name.hashCode() : 0;
        result1 = 31 * result1 + (cron != null ? cron.hashCode() : 0);
        result1 = 31 * result1 + (type != null ? type.hashCode() : 0);
        result1 = 31 * result1 + (query != null ? query.hashCode() : 0);
        result1 = 31 * result1 + (timezone != null ? timezone.hashCode() : 0);
        result1 = 31 * result1 + (delay != null ? delay.hashCode() : 0);
        result1 = 31 * result1 + (database != null ? database.hashCode() : 0);
        result1 = 31 * result1 + (priority != null ? priority.hashCode() : 0);
        result1 = 31 * result1 + (retryLimit != null ? retryLimit.hashCode() : 0);
        result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
        result1 = 31 * result1 + (engineVersion != null ? engineVersion.hashCode() : 0);
        return result1;
    }
}
