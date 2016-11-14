package com.treasuredata.client.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

import java.util.Date;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableTDSavedQueryStartRequest.class)
@JsonDeserialize(as = ImmutableTDSavedQueryStartRequest.class)
public abstract class TDSavedQueryStartRequest
{
    public abstract String name();
    public abstract Optional<Long> id();
    public abstract Date scheduledTime();
    public abstract Optional<Integer> num();
    public abstract Optional<String> domainKey();

    public interface Builder
    {
        Builder name(String name);
        Builder id(long id);
        Builder scheduledTime(Date scheduledTime);
        Builder num(Optional<Integer> num);
        Builder num(int num);
        Builder domainKey(Optional<String> domainKey);
        Builder domainKey(String domainKey);
        TDSavedQueryStartRequest build();
    }

    public static TDSavedQueryStartRequest.Builder builder()
    {
        return ImmutableTDSavedQueryStartRequest.builder();
    }
}
