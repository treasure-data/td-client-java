package com.treasuredata.client.model;

import com.google.common.base.Optional;

public class TDBulkLoadSessionStartRequestBuilder
{
    private Long scheduledTime;

    TDBulkLoadSessionStartRequestBuilder()
    {
    }

    public Optional<Long> getScheduledTime()
    {
        return Optional.fromNullable(scheduledTime);
    }

    public TDBulkLoadSessionStartRequestBuilder setScheduledTime(Long scheduledTime)
    {
        this.scheduledTime = scheduledTime;
        return this;
    }

    public TDBulkLoadSessionStartRequestBuilder setScheduledTime(Optional<Long> scheduledTime)
    {
        this.scheduledTime = scheduledTime.orNull();
        return this;
    }

    public TDBulkLoadSessionStartRequest build()
    {
        return new TDBulkLoadSessionStartRequest(this);
    }
}
