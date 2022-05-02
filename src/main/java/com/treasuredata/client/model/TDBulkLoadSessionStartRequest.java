package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class TDBulkLoadSessionStartRequest
{
    private final String scheduledTime;
    private final String domainKey;

    @JsonCreator
    TDBulkLoadSessionStartRequest(
            @JsonProperty("scheduled_time") String scheduledTime,
            @JsonProperty("domain_key") String domainKey
            )
    {
        this.scheduledTime = scheduledTime;
        this.domainKey = domainKey;
    }

    TDBulkLoadSessionStartRequest(TDBulkLoadSessionStartRequestBuilder builder)
    {
        this.scheduledTime = builder.getScheduledTime().orElse(null);
        this.domainKey = builder.getDomainKey().orElse(null);
    }

    @JsonProperty("scheduled_time")
    public Optional<String> getScheduledTime()
    {
        return Optional.ofNullable(scheduledTime);
    }

    @JsonProperty("domain_key")
    public Optional<String> getDomainKey()
    {
        return Optional.ofNullable(domainKey);
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

        TDBulkLoadSessionStartRequest that = (TDBulkLoadSessionStartRequest) o;

        return scheduledTime != null ? scheduledTime.equals(that.scheduledTime) : that.scheduledTime == null;
    }

    @Override
    public int hashCode()
    {
        return scheduledTime != null ? scheduledTime.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "TDBulkLoadSessionStartRequest{" +
                "scheduledTime='" + scheduledTime + '\'' +
                ", domainKey='" + domainKey + '\'' +
                '}';
    }

    public static TDBulkLoadSessionStartRequestBuilder builder()
    {
        return new TDBulkLoadSessionStartRequestBuilder();
    }

    public static TDBulkLoadSessionStartRequest of()
    {
        return builder().build();
    }
}
