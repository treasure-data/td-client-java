package com.treasuredata.client.model;

import org.immutables.builder.Builder;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Style(typeBuilder = "TDExportResultJobRequestBuilder")
public class TDExportResultJobRequest
{
    private final String jobId;
    private final String resultOutput;
    private final String resultConnectionId;
    private final String resultConnectionSettings;

    private TDExportResultJobRequest(String jobId, String resultOutput, String reseultConnectionId, String resultConnectionSettings)
    {
        this.jobId = jobId;
        this.resultOutput = resultOutput;
        this.resultConnectionId = reseultConnectionId;
        this.resultConnectionSettings = resultConnectionSettings;
    }

    public String getJobId()
    {
        return jobId;
    }

    public String getResultOutput()
    {
        return resultOutput;
    }

    public String getResultConnectionId()
    {
        return resultConnectionId;
    }

    public String getResultConnectionSettings()
    {
        return resultConnectionSettings;
    }

    @Builder.Factory
    static TDExportResultJobRequest of(String jobId,
            Optional<String> resultOutput,
            Optional<String> resultConnectionId,
            Optional<String> resultConnectionSettings)
    {
        return new TDExportResultJobRequest(jobId,
                resultOutput.orElse(""),
                resultConnectionId.orElse(""),
                resultConnectionSettings.orElse(""));
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
                ", resultConnectionId='" + resultConnectionId + '\'' +
                ", resultConnectionSettings='" + resultConnectionSettings + '\'' +
                '}';
    }
}
