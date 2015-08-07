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
package com.treasuredata.client.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

/**
 *
 */
public class TDJob
{
    public static enum Type
    {
        HIVE("hive"), MAPRED("mapred"), PRESTO("presto"), UNKNOWN("none");
        private final String type;

        private Type(String type)
        {
            this.type = type;
        }

        public String getType()
        {
            return type;
        }

        public static Type fromString(String typeName)
        {
            for (Type t : values()) {
                if (t.type.equals(typeName)) {
                    return t;
                }
            }
            return UNKNOWN;
        }
    }

    public static enum Priority
    {
        VERYLOW(-2), LOW(-1), NORMAL(0), HIGH(1), VERYHIGH(2);
        private final int priority;

        private Priority(int priority)
        {
            this.priority = priority;
        }

        public int getPriority()
        {
            return priority;
        }

        public static Priority fromInt(int priority)
        {
            for (Priority p : values()) {
                if (p.priority == priority) {
                    return p;
                }
            }
            // For unknown property value, returns the default value NORMAL
            return NORMAL;
        }
    }

    public static enum Status
    {
        QUEUED, BOOTING, RUNNING, SUCCESS, ERROR, KILLED, UNKNOWN;

        @JsonCreator
        public static Status fromString(String s)
        {
            return valueOf(s.toUpperCase());
        }
    }

    private static class Debug
    {
        private final String cmdout;
        private final String stderr;

        public Debug(String cmdout, String stderr)
        {
            this.cmdout = cmdout;
            this.stderr = stderr;
        }

        public String getCmdout()
        {
            return cmdout;
        }

        public String getStderr()
        {
            return stderr;
        }
    }

    private final String jobId;
    private final Status status;
    private final TDQuery query;
    private final String createdAt;
    private final String startAt;
    private final Optional<String> endAt;
    private final Optional<String> resultSchema;  // only for Hive
    private final String database;
    private final String result;
    private final String url;
    private final String userName;
    private final long duration;
    private final long resultSize;

    public TDJob(
            @JsonProperty("job_id") String jobId,
            @JsonProperty("status") Status status,
            @JsonProperty("query") TDQuery query,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("start_at") String startAt,
            @JsonProperty("end_at") Optional<String> endAt,
            @JsonProperty("result_schema") Optional<String> resultSchema,
            @JsonProperty("database") String database,
            @JsonProperty("result") String result,
            @JsonProperty("url") String url,
            @JsonProperty("user_name") String userName,
            @JsonProperty("duration") long duration,
            @JsonProperty("result_size") long resultSize)
    {
        this.jobId = jobId;
        this.status = status;
        this.query = query;
        this.createdAt = createdAt;
        this.startAt = startAt;
        this.endAt = endAt;
        this.resultSchema = resultSchema;
        this.database = database;
        this.result = result;
        this.url = url;
        this.userName = userName;
        this.duration = duration;
        this.resultSize = resultSize;
    }

    public String getJobId()
    {
        return jobId;
    }

    public Status getStatus()
    {
        return status;
    }

    public TDQuery getQuery()
    {
        return query;
    }

    public String getCreatedAt()
    {
        return createdAt;
    }

    public String getStartAt()
    {
        return startAt;
    }

    public Optional<String> getEndAt()
    {
        return endAt;
    }

    public Optional<String> getResultSchema()
    {
        return resultSchema;
    }

    public String getDatabase()
    {
        return database;
    }

    public String getResult()
    {
        return result;
    }

    public String getUrl()
    {
        return url;
    }

    public String getUserName()
    {
        return userName;
    }

    public long getDuration()
    {
        return duration;
    }

    public long getResultSize()
    {
        return resultSize;
    }

    @Override
    public String toString()
    {
        return "TDJob{" +
                "jobId='" + jobId + '\'' +
                ", query='" + query + '\'' +
                ", status=" + status +
                ", createdAt='" + createdAt + '\'' +
                ", startAt='" + startAt + '\'' +
                ", endAt=" + endAt +
                ", database='" + database + '\'' +
                ", duration=" + duration +
                ", userName='" + userName + '\'' +
                ", url='" + url + '\'' +
                ", result='" + result + '\'' +
                ", resultSchema=" + resultSchema +
                ", resultSize=" + resultSize +
                '}';
    }
}
