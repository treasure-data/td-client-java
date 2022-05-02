package com.treasuredata.client.model;

import java.util.Optional;

public class TDBulkLoadSessionStartRequestBuilder
{
    private String scheduledTime;
    private String domainKey;

    TDBulkLoadSessionStartRequestBuilder()
    {
    }

    public Optional<String> getScheduledTime()
    {
        return Optional.ofNullable(scheduledTime);
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
        this.scheduledTime = scheduledTime.orElse(null);
        return this;
    }

    public Optional<String> getDomainKey()
    {
        return Optional.ofNullable(domainKey);
    }

    public TDBulkLoadSessionStartRequestBuilder setDomainKey(String domainKey)
    {
        this.domainKey = domainKey;
        return this;
    }

    public TDBulkLoadSessionStartRequestBuilder setDomainKey(Optional<String> domainKey)
    {
        return setDomainKey(domainKey.orElse(null));
    }

    public TDBulkLoadSessionStartRequest build()
    {
        return new TDBulkLoadSessionStartRequest(this);
    }
}
