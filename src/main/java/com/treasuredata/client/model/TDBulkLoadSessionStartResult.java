package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TDBulkLoadSessionStartResult
{
    private final String jobId;

    @JsonCreator
    TDBulkLoadSessionStartResult(
            @JsonProperty("job_id") String jobId)
    {
        this.jobId = jobId;
    }

    @JsonProperty("job_id")
    public String getJobId()
    {
        return jobId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TDBulkLoadSessionStartResult that = (TDBulkLoadSessionStartResult) o;

        return jobId != null ? jobId.equals(that.jobId) : that.jobId == null;
    }

    @Override
    public int hashCode()
    {
        return jobId != null ? jobId.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "TDBulkLoadSessionStartResult{" +
                "jobId='" + jobId + '\'' +
                '}';
    }

    public static TDBulkLoadSessionStartResult of(String jobId)
    {
        return new TDBulkLoadSessionStartResult(jobId);
    }
}
