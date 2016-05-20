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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

public class TDJobRequestBuilder
{
    private String database;
    private TDJob.Type type = TDJob.Type.PRESTO;
    private String query;
    private TDJob.Priority priority = TDJob.Priority.NORMAL;
    private String result;
    private Optional<Integer> retryLimit = Optional.absent();
    private String poolName;
    private Optional<String> table = Optional.absent();
    private Optional<ObjectNode> config = Optional.absent();
    private Optional<Long> scheduledTime = Optional.absent();
    private Optional<String> domainKey = Optional.absent();

    public TDJobRequestBuilder setResultOutput(String result)
    {
        this.result = result;
        return this;
    }

    public Optional<String> getResultOutput()
    {
        return Optional.fromNullable(result);
    }

    public TDJobRequestBuilder setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    public String getDatabase()
    {
        return database;
    }

    public TDJobRequestBuilder setType(TDJob.Type type)
    {
        this.type = type;
        return this;
    }

    public TDJobRequestBuilder setType(String type)
    {
        this.type = TDJob.Type.fromString(type);
        return this;
    }

    public TDJob.Type getType()
    {
        return type;
    }

    public TDJobRequestBuilder setQuery(String query)
    {
        this.query = query;
        return this;
    }

    public String getQuery()
    {
        return query;
    }

    public TDJobRequestBuilder setPriority(TDJob.Priority priority)
    {
        this.priority = priority;
        return this;
    }

    public TDJobRequestBuilder setPriority(int priority)
    {
        this.priority = TDJob.Priority.fromInt(priority);
        return this;
    }

    public TDJob.Priority getPriority()
    {
        return priority;
    }

    public TDJobRequestBuilder setRetryLimit(int retryLimit)
    {
        this.retryLimit = Optional.of(retryLimit);
        return this;
    }

    public Optional<Integer> getRetryLimit()
    {
        return retryLimit;
    }

    public TDJobRequestBuilder setPoolName(String poolName)
    {
        this.poolName = poolName;
        return this;
    }

    public Optional<String> getPoolName()
    {
        return Optional.fromNullable(poolName);
    }

    public TDJobRequestBuilder setTable(String table)
    {
        this.table = Optional.of(table);
        return this;
    }

    public Optional<String> getTable()
    {
        return table;
    }

    public TDJobRequestBuilder setConfig(ObjectNode config)
    {
        this.config = Optional.of(config);
        return this;
    }

    public Optional<ObjectNode> getConfig()
    {
        return config;
    }

    public TDJobRequestBuilder setScheduledTime(Long scheduledTime)
    {
        return setScheduledTime(Optional.fromNullable(scheduledTime));
    }

    public TDJobRequestBuilder setScheduledTime(Optional<Long> scheduledTime)
    {
        this.scheduledTime = scheduledTime;
        return this;
    }

    public Optional<Long> getScheduledTime()
    {
        return scheduledTime;
    }

    public Optional<String> getDomainKey()
    {
        return domainKey;
    }

    public TDJobRequestBuilder setDomainKey(Optional<String> domainKey)
    {
        this.domainKey = domainKey;
        return this;
    }

    public TDJobRequestBuilder setDomainKey(String domainKey)
    {
        return setDomainKey(Optional.fromNullable(domainKey));
    }

    public TDJobRequest createTDJobRequest()
    {
        return TDJobRequest.of(this);
    }
}
