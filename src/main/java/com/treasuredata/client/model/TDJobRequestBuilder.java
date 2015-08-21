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

public class TDJobRequestBuilder
{
    private String database;
    private TDJob.Type type = TDJob.Type.PRESTO;
    private String query;
    private TDJob.Priority priority = TDJob.Priority.NORMAL;
    private String result;
    private int retryLimit = 10;

    public void setResult(String result)
    {
        this.result = result;
    }

    public TDJobRequestBuilder setDatabase(String database)
    {
        this.database = database;
        return this;
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

    public TDJobRequestBuilder setQuery(String query)
    {
        this.query = query;
        return this;
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

    public TDJobRequestBuilder setRetryLimit(int retryLimit)
    {
        this.retryLimit = retryLimit;
        return this;
    }

    public TDJobRequest createTDJobRequest()
    {
        return new TDJobRequest(database, type, query, priority, Optional.fromNullable(result), retryLimit);
    }
}