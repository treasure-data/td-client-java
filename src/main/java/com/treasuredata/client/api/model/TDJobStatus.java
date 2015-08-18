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

/**
 *
 */
public class TDJobStatus
{
    private final TDJob.Status status;
    private final long resultSize;
    private final long duration;
    private final String jobId;
    private final String createdAt;
    private final String updatedAt;
    private final String startedAt;
    private final String endAt;

    @JsonCreator
    public TDJobStatus(
            @JsonProperty("status") TDJob.Status status,
            @JsonProperty("result_size") long resultSize,
            @JsonProperty("duration") long duration,
            @JsonProperty("job_id") String jobId,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt,
            @JsonProperty("started_at") String startedAt,
            @JsonProperty("end_at") String endAt)
    {
        this.status = status;
        this.resultSize = resultSize;
        this.duration = duration;
        this.jobId = jobId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.startedAt = startedAt;
        this.endAt = endAt;
    }

    public TDJob.Status getStatus()
    {
        return status;
    }

    public long getResultSize()
    {
        return resultSize;
    }

    public long getDuration()
    {
        return duration;
    }

    public String getJobId()
    {
        return jobId;
    }

    public String getCreatedAt()
    {
        return createdAt;
    }

    public String getUpdatedAt()
    {
        return updatedAt;
    }

    public String getStartedAt()
    {
        return startedAt;
    }

    public String getEndAt()
    {
        return endAt;
    }

    @Override
    public String toString()
    {
        return "TDJobStatus{" +
                "status=" + status +
                ", resultSize=" + resultSize +
                ", duration=" + duration +
                ", jobId='" + jobId + '\'' +
                ", createdAt='" + createdAt + '\'' +
                ", updatedAt='" + updatedAt + '\'' +
                ", startedAt='" + startedAt + '\'' +
                ", endAt='" + endAt + '\'' +
                '}';
    }
}
