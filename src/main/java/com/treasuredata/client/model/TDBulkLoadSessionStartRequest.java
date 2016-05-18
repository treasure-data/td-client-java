package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class TDBulkLoadSessionStartRequest
{
    private final Long scheduledTime;

    @JsonCreator
    TDBulkLoadSessionStartRequest(@JsonProperty("scheduled_time") Long scheduledTime)
    {
        this.scheduledTime = scheduledTime;
    }

    TDBulkLoadSessionStartRequest(TDBulkLoadSessionStartRequestBuilder builder)
    {
        this.scheduledTime = builder.getScheduledTime().orNull();
    }

    @JsonProperty("scheduled_time")
    public Optional<Long> getScheduledTime()
    {
        return Optional.fromNullable(scheduledTime);
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
                "scheduledTime=" + scheduledTime +
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
