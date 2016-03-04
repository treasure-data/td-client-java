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
package com.treasuredata.client.model.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.treasuredata.client.model.TDJob;

import java.util.List;

/**
 *
 */
public class TDScheduleRunResult
{
    public static class Job
    {
        private final String jobId;
        private final TDJob.Type jobType;
        private final String scheduledAt;

        public Job(
                @JsonProperty("job_id") String jobId,
                @JsonProperty("job_type") TDJob.Type jobType,
                @JsonProperty("scheduled_at") String scheduledAt)
        {
            this.jobId = jobId;
            this.jobType = jobType;
            this.scheduledAt = scheduledAt;
        }

        @JsonProperty("job_id")
        public String getJobId()
        {
            return jobId;
        }

        @JsonProperty("type")
        public TDJob.Type getType()
        {
            return jobType;
        }

        @JsonProperty("scheduled_at")
        public String getScheduledAt()
        {
            return scheduledAt;
        }

        @Override
        public String toString()
        {
            return "Job{" +
                    "jobId='" + jobId + '\'' +
                    '}';
        }
    }

    private final List<Job> jobs;

    @JsonCreator
    public TDScheduleRunResult(@JsonProperty("jobs") List<Job> jobs)
    {
        this.jobs = ImmutableList.copyOf(jobs);
    }

    @JsonProperty("jobs")
    public List<Job> getJobs()
    {
        return jobs;
    }

    @Override
    public String toString()
    {
        return "TDScheduleRunResult{" +
                "jobs=" + jobs +
                '}';
    }
}
