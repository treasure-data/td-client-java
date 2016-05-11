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

import com.google.common.base.Optional;
import com.treasuredata.client.TDClientException;

/**
 *
 */
public class TDSavedQueryUpdateRequestBuilder
{
    private Optional<String> name = Optional.absent();
    private Optional<String> cron = Optional.absent();
    private Optional<TDJob.Type> type = Optional.absent();
    private Optional<String> query = Optional.absent();
    private Optional<String> timezone = Optional.absent();
    private Optional<Long> delay = Optional.absent();
    private Optional<String> database = Optional.absent();
    private Optional<Integer> priority = Optional.absent();
    private Optional<Integer> retryLimit = Optional.absent();
    private Optional<String> result = Optional.absent();

    TDSavedQueryUpdateRequestBuilder()
    {
    }

    public static <T> void checkPresence(Optional<T> opt, String errorMessage)
    {
        if (!opt.isPresent()) {
            throw new TDClientException(TDClientException.ErrorType.INVALID_INPUT, errorMessage);
        }
    }

    public TDSavedQueryUpdateRequest build()
    {
        return new TDSavedQueryUpdateRequest(
                name,
                cron,
                type,
                query,
                timezone,
                delay,
                database,
                priority,
                retryLimit,
                result
        );
    }

    public TDSavedQueryUpdateRequestBuilder setName(String name)
    {
        this.name = Optional.of(name);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setCron(String cron)
    {
        this.cron = Optional.of(cron);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setType(TDJob.Type type)
    {
        this.type = Optional.of(type);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setQuery(String query)
    {
        this.query = Optional.of(query);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setTimezone(String timezone)
    {
        this.timezone = Optional.of(timezone);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setDelay(long delay)
    {
        this.delay = Optional.of(delay);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setDatabase(String database)
    {
        this.database = Optional.of(database);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setPriority(int priority)
    {
        this.priority = Optional.of(priority);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setRetryLimit(int retryLimit)
    {
        this.retryLimit = Optional.of(retryLimit);
        return this;
    }

    public TDSavedQueryUpdateRequestBuilder setResult(String result)
    {
        this.result = Optional.of(result);
        return this;
    }
}
