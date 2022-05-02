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

import java.util.Optional;

import com.treasuredata.client.TDClientException;

public class TDSavedQueryBuilder
{
    private Optional<String> name = Optional.empty();
    private Optional<String> cron = Optional.empty();
    private Optional<TDJob.Type> type = Optional.empty();
    private Optional<String> query = Optional.empty();
    private Optional<String> timezone = Optional.empty();
    private Optional<Long> delay = Optional.empty();
    private Optional<String> database = Optional.empty();
    private Optional<Integer> priority = Optional.empty();
    private Optional<Integer> retryLimit = Optional.empty();
    private Optional<String> result = Optional.empty();
    private Optional<TDJob.EngineVersion> engineVersion = Optional.empty();

    public TDSavedQueryBuilder setName(String name)
    {
        this.name = Optional.of(name);
        return this;
    }

    public TDSavedQueryBuilder setCron(String cron)
    {
        this.cron = Optional.of(cron);
        return this;
    }

    public TDSavedQueryBuilder setType(TDJob.Type type)
    {
        this.type = Optional.of(type);
        return this;
    }

    public TDSavedQueryBuilder setQuery(String query)
    {
        this.query = Optional.of(query);
        return this;
    }

    public TDSavedQueryBuilder setTimezone(String timezone)
    {
        this.timezone = Optional.of(timezone);
        return this;
    }

    public TDSavedQueryBuilder setDelay(long delay)
    {
        this.delay = Optional.of(delay);
        return this;
    }

    public TDSavedQueryBuilder setDatabase(String database)
    {
        this.database = Optional.of(database);
        return this;
    }

    public TDSavedQueryBuilder setPriority(int priority)
    {
        this.priority = Optional.of(priority);
        return this;
    }

    public TDSavedQueryBuilder setRetryLimit(int retryLimit)
    {
        this.retryLimit = Optional.of(retryLimit);
        return this;
    }

    public TDSavedQueryBuilder setResult(String result)
    {
        this.result = Optional.of(result);
        return this;
    }

    public TDSavedQueryBuilder setEngineVersion(TDJob.EngineVersion engineVersion)
    {
        this.engineVersion = Optional.ofNullable(engineVersion);
        return this;
    }

    private static <T> void checkPresence(Optional<T> opt, String errorMessage)
    {
        if (!opt.isPresent()) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, errorMessage);
        }
    }

    public TDSaveQueryRequest build()
    {
        checkPresence(name, "name is not set");
        checkPresence(type, "job type is not set");
        checkPresence(query, "query is not set");
        checkPresence(timezone, "timezone is not set. Use UTC, US/Pacific, Asia/Tokyo, etc.");
        checkPresence(database, "database is not set");

        return new TDSaveQueryRequest(
                name.get(),
                cron.orElse(""),
                type.get(),
                query.get(),
                timezone.get(),
                delay.orElse(0L),
                database.get(),
                priority.orElse(TDJob.Priority.NORMAL.toInt()),
                retryLimit.orElse(0),
                result.orElse(""),
                engineVersion.orElse(null)
        );
    }

    protected TDSavedQueryBuilder(String name)
    {
        setName(name);
    }
}
