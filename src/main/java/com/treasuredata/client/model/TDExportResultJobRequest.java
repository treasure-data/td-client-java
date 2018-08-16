package com.treasuredata.client.model;

import org.immutables.builder.Builder;
import org.immutables.value.Value;

@Value.Style(typeBuilder = "TDExportResultJobRequestBuilder")
public class TDExportResultJobRequest
{
    private final String jobId;
    private final String resultOutput;

    private TDExportResultJobRequest(String jobId, String resultOutput)
    {
        this.jobId = jobId;
        this.resultOutput = resultOutput;
    }

    public String getJobId()
    {
        return jobId;
    }

    public String getResultOutput()
    {
        return resultOutput;
    }

    @Builder.Factory
    static TDExportResultJobRequest of(String jobId, String resultOutput)
    {
        return new TDExportResultJobRequest(jobId, resultOutput);
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
                ", resultOutput='" + resultOutput + '\'' +
                '}';
    }
}
