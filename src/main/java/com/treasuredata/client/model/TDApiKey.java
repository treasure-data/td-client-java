package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableTDApiKey.class)
@JsonDeserialize(as = ImmutableTDApiKey.class)
public abstract class TDApiKey
{
    @JsonProperty("key_type") public abstract String getKeyType();
    @JsonProperty("account_id") public abstract Integer getAccountId();
    @JsonProperty("user_id") public abstract Integer getUserId();
    @JsonProperty("administrator") public abstract boolean isAdministrator();

    public interface Builder
    {
        Builder keyType(String keyType);
        Builder accountId(Integer accountId);
        Builder userId(Integer userId);
        Builder isAdministrator(boolean administrator);
        TDApiKey build();
    }

    public static Builder builder()
    {
        return ImmutableTDApiKey.builder();
    }
}
