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
import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import java.util.List;

/**
 *
 */
public class TDJobList
{
    private final List<TDJob> jobs;
    private final int count;
    private final Optional<Long> from;
    private final Optional<Long> to;

    public TDJobList(
            @JsonProperty("jobs") List<TDJob> jobs,
            @JsonProperty("count") int count,
            @JsonProperty("from") Optional<Long> from,
            @JsonProperty("to") Optional<Long> to)
    {
        this.jobs = jobs;
        this.count = count;
        this.from = from;
        this.to = to;
    }

    public List<TDJob> getJobs()
    {
        return jobs;
    }

    public int getCount()
    {
        return count;
    }

    public Optional<Long> getFrom()
    {
        return from;
    }

    public Optional<Long> getTo()
    {
        return to;
    }

    @Override
    public String toString()
    {
        return Joiner.on("\n").join(jobs);
    }
}
