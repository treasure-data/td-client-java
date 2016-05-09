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

import java.util.List;

/**
 *
 */
public class TDSavedQuery
{
    public static class TDSavedQueryList
    {
        private final List<TDSavedQuery> schedules;

        public TDSavedQueryList(@JsonProperty("schedules") List<TDSavedQuery> schedules)
        {
            this.schedules = schedules;
        }

        public List<TDSavedQuery> getSchedules()
        {
            return schedules;
        }
    }

    private final String name;
    private final String cron;
    private final TDJob.Type type;
    private final String query;
    private final String timezone;
    private final long delay;
    private final String database;
    private final String userName;
    private final int priority;
    private final int retryLimit;
    private final String result;
    private final String nextTime;

    public TDSavedQuery(
            @JsonProperty("name") String name,
            @JsonProperty("cron") String cron,
            @JsonProperty("type") TDJob.Type type,
            @JsonProperty("query") String query,
            @JsonProperty("timezone") String timezone,
            @JsonProperty("delay") long delay,
            @JsonProperty("database") String database,
            @JsonProperty("user_name") String userName,
            @JsonProperty("priority") int priority,
            @JsonProperty("retry_limit") int retryLimit,
            @JsonProperty("result") String result,
            @JsonProperty("next_time") String nextTime)
    {
        this.name = name;
        this.cron = cron;
        this.type = type;
        this.query = query;
        this.timezone = timezone;
        this.delay = delay;
        this.database = database;
        this.userName = userName;
        this.priority = priority;
        this.retryLimit = retryLimit;
        this.result = result;
        this.nextTime = nextTime;
    }

    public String getName()
    {
        return name;
    }

    public String getCron()
    {
        return cron;
    }

    public TDJob.Type getType()
    {
        return type;
    }

    public String getQuery()
    {
        return query;
    }

    public String getTimezone()
    {
        return timezone;
    }

    public long getDelay()
    {
        return delay;
    }

    public String getDatabase()
    {
        return database;
    }

    public String getUserName()
    {
        return userName;
    }

    public int getPriority()
    {
        return priority;
    }

    public int getRetryLimit()
    {
        return retryLimit;
    }

    /**
     * Get the result target URL. This returns an empty string if nothing is specified
     *
     * @return Get the result target URL
     */
    public String getResult()
    {
        return result;
    }

    public String getNextTime()
    {
        return nextTime;
    }

    @Override
    public String toString()
    {
        return "TDSavedQuery{" +
                "name='" + name + '\'' +
                ", cron='" + cron + '\'' +
                ", type=" + type +
                ", query='" + query + '\'' +
                ", timezone='" + timezone + '\'' +
                ", delay=" + delay +
                ", database='" + database + '\'' +
                ", userName='" + userName + '\'' +
                ", priority=" + priority +
                ", retryLimit=" + retryLimit +
                ", result='" + result + '\'' +
                ", nextTime='" + nextTime + '\'' +
                '}';
    }
}
