package com.treasuredata.client.model;

import com.google.common.base.Optional;

public class TDBulkLoadSessionStartRequestBuilder
{
    private String scheduledTime;

    TDBulkLoadSessionStartRequestBuilder()
    {
    }

    public Optional<String> getScheduledTime()
    {
        return Optional.fromNullable(scheduledTime);
    }

    public TDBulkLoadSessionStartRequestBuilder setScheduledTime(String scheduledTime)
    {
        this.scheduledTime = scheduledTime;
        return this;
    }

    public TDBulkLoadSessionStartRequestBuilder setScheduledTime(long scheduledTime)
    {
        return setScheduledTime(String.valueOf(scheduledTime));
    }

    public TDBulkLoadSessionStartRequestBuilder setScheduledTime(Optional<String> scheduledTime)
    {
        this.scheduledTime = scheduledTime.orNull();
        return this;
    }

    public TDBulkLoadSessionStartRequest build()
    {
        return new TDBulkLoadSessionStartRequest(this);
    }
}
