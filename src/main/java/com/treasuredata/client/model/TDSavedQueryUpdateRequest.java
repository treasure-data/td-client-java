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

    TDSavedQueryUpdateRequest(Optional<String> name, Optional<String> cron, Optional<TDJob.Type> type, Optional<String> query, Optional<String> timezone, Optional<Long> delay, Optional<String> database, Optional<Integer> priority, Optional<Integer> retryLimit, Optional<String> result)
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
    }

    /**
     * Apply this update to the given TDSaveQuery objects and create a new TDSaveQueryRequest object.
     *
     * @param base
     * @return
     */
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
                result.or(base.getResult()));
    }
}
