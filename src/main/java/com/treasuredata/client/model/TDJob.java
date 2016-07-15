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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Optional;

/**
 *
 */
public class TDJob
{
    public static enum Type
    {
        HIVE("hive"), MAPRED("mapred"), PRESTO("presto"), PIG("pig"), BULKLOAD("bulkload"), EXPORT("export"), UNKNOWN("none");
        private final String type;

        private Type(String type)
        {
            this.type = type;
        }

        @JsonValue
        public String getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return type;
        }

        @JsonCreator
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

        public int toInt()
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

        public boolean isFinished()
        {
            return this == SUCCESS ||
                    this == ERROR ||
                    this == KILLED;
        }
    }

    private static class Debug
    {
        private final Optional<String> cmdout;
        private final Optional<String> stderr;

        @JsonCreator
        public Debug(@JsonProperty("cmdout") Optional<String> cmdout, @JsonProperty("stderr") Optional<String> stderr)
        {
            this.cmdout = cmdout;
            this.stderr = stderr;
        }

        public String getCmdout()
        {
            return cmdout.or("");
        }

        public String getStderr()
        {
            return stderr.or("");
        }

        @Override
        public String toString()
        {
            return "Debug{" +
                    "cmdout='" + cmdout + '\'' +
                    ", stderr='" + stderr + '\'' +
                    '}';
        }
    }

    private final String jobId;
    private final Status status;
    private final Type type;
    private final String query;
    private final String createdAt;
    private final String startAt;
    private final String updatedAt;
    private final String endAt;
    private final Optional<String> resultSchema;  // only for Hive
    private final String database;
    private final String result;
    private final String url;
    private final String userName;
    private final long duration;
    private final long resultSize;
    private final Optional<Debug> debug;

    @JsonCreator
    static TDJob createTDJobV3(
            @JsonProperty("job_id") String jobId,
            @JsonProperty("status") Status status,
            @JsonProperty("type") Type type,
            @JsonProperty("query") TDQuery query,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("start_at") String startAt,
            @JsonProperty("updated_at") String updatedAt,
            @JsonProperty("end_at") String endAt,
            @JsonProperty("hive_result_schema") Optional<String> resultSchema,
            @JsonProperty("database") String database,
            @JsonProperty("result") String result,
            @JsonProperty("url") String url,
            @JsonProperty("user_name") String userName,
            @JsonProperty("duration") long duration,
            @JsonProperty("result_size") long resultSize,
            @JsonProperty("debug") Optional<Debug> debug)
    {
        return new TDJob(jobId, status, type, query.getQuery(), createdAt, startAt, updatedAt, endAt, resultSchema, database, result, url, userName, duration, resultSize, debug);
    }

    public TDJob(String jobId,
            Status status,
            Type type,
            String query,
            String createdAt,
            String startAt,
            String updatedAt,
            String endAt,
            Optional<String> resultSchema,
            String database,
            String result,
            String url,
            String userName,
            long duration,
            long resultSize,
            Optional<Debug> debug
    )
    {
        this.jobId = jobId;
        this.status = status;
        this.type = type;
        this.query = query;
        this.createdAt = createdAt;
        this.startAt = startAt;
        this.updatedAt = updatedAt;
        this.endAt = endAt;
        this.resultSchema = resultSchema;
        this.database = database;
        this.result = result;
        this.url = url;
        this.userName = userName;
        this.duration = duration;
        this.resultSize = resultSize;
        this.debug = debug;
    }

    public String getJobId()
    {
        return jobId;
    }

    public Status getStatus()
    {
        return status;
    }

    public Type getType()
    {
        return type;
    }

    public String getQuery()
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

    public String getEndAt()
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

    public Optional<Debug> getDebug()
    {
        return debug;
    }

    /**
     * A short cut for reading cmdout message
     *
     * @return
     */
    public String getCmdOut()
    {
        return debug.transform(new Function<Debug, String>()
        {
            @Override
            public String apply(Debug input)
            {
                return input.getCmdout();
            }
        }).or("");
    }

    /**
     * A short cut for reading stderr messsage
     *
     * @return
     */
    public String getStdErr()
    {
        return debug.transform(new Function<Debug, String>()
        {
            @Override
            public String apply(Debug input)
            {
                return input.getStderr();
            }
        }).or("");
    }

    @Override
    public String toString()
    {
        return "TDJob{" +
                "jobId='" + jobId + '\'' +
                ", status=" + status +
                ", type=" + type +
                ", query=" + query +
                ", createdAt='" + createdAt + '\'' +
                ", startAt='" + startAt + '\'' +
                ", updatedAt='" + updatedAt + '\'' +
                ", endAt='" + endAt + '\'' +
                ", resultSchema=" + resultSchema +
                ", database='" + database + '\'' +
                ", result='" + result + '\'' +
                ", url='" + url + '\'' +
                ", userName='" + userName + '\'' +
                ", duration=" + duration +
                ", resultSize=" + resultSize +
                ", debug=" + debug +
                '}';
    }
}
