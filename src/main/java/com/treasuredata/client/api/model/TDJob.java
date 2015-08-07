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

import java.util.Optional;

/**
 *
 */
public class TDJob
{
    public static enum Type {
        HIVE("hive"), MAPRED("mapred"), PRESTO("presto"), UNKNOWN("none");

        private final String type;

        private Type(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public static Type fromString(String typeName) {
            for(Type t : values()) {
                if(t.type.equals(typeName)) {
                    return t;
                }
            }
            return UNKNOWN;
        }
    }

    public static enum Priority {
        VERYLOW(-2), LOW(-1), NORMAL(0), HIGH(1), VERYHIGH(2);

        private final int priority;

        private Priority(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }

        public static Priority fromInt(int priority) {
            for(Priority p : values()) {
                if(p.priority == priority) {
                    return p;
                }
            }
            // For unknown property value, returns the default value NORMAL
            return NORMAL;
        }
    }

    public static enum Status {
        QUEUED, BOOTING, RUNNING, SUCCESS, ERROR, KILLED, UNKNOWN;
    }

    private static class Debug {
        private final String cmdout;
        private final String stderr;

        public Debug(String cmdout, String stderr) {
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
    private final String createdAt;
    private final String startAt;
    private final Optional<String> endAt;
    private final Optional<String> resultSchema;  // only for Hive
    private final Debug debug;

    public TDJob(String jobId, Status status, String createdAt, String startAt, Optional<String> endAt, Optional<String> resultSchema, Debug debug)
    {
        this.jobId = jobId;
        this.status = status;
        this.createdAt = createdAt;
        this.startAt = startAt;
        this.endAt = endAt;
        this.resultSchema = resultSchema;
        this.debug = debug;
    }

    public Status getStatus()
    {
        return status;
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

    public Debug getDebug()
    {
        return debug;
    }
}
