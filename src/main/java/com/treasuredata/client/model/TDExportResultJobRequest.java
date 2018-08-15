package com.treasuredata.client.model;

import org.immutables.builder.Builder;
import org.immutables.value.Value;

@Value.Style(typeBuilder = "TDExportResultJobRequestBuilder")
public class TDExportResultJobRequest
{
    private final String jobId;
    private final String result;

    private TDExportResultJobRequest(String jobId, String result)
    {
        this.jobId = jobId;
        this.result = result;
    }

    public String getJobId()
    {
        return jobId;
    }

    public String getResult()
    {
        return result;
    }

    @Builder.Factory
    static TDExportResultJobRequest of(String jobId, String result)
    {
        return new TDExportResultJobRequest(jobId, result);
    }

    public static TDExportResultJobRequestBuilder builder()
    {
        return new TDExportResultJobRequestBuilder();
    }

    @Override
    public String toString()
    {
        return "TDExportResultJobRequest{" +
                "jobId='" + jobId + '\'' +
                ", result='" + result + '\'' +
                '}';
    }
}
