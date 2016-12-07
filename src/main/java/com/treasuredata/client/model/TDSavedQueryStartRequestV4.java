package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableTDSavedQueryStartRequestV4.class)
@JsonDeserialize(as = ImmutableTDSavedQueryStartRequestV4.class)
public abstract class TDSavedQueryStartRequestV4
{
    @JsonProperty("scheduled_time") public abstract String scheduledTime();
    @JsonProperty("domain_key") public abstract Optional<String> domainKey();

    public static TDSavedQueryStartRequestV4 from(TDSavedQueryStartRequest request)
    {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(tz);

        return builder()
                .domainKey(request.domainKey())
                .scheduledTime(df.format(request.scheduledTime()))
                .build();
    }

    public interface Builder
    {
        Builder scheduledTime(String scheduledTime);
        Builder domainKey(Optional<String> domainKey);
        Builder domainKey(String domainKey);
        TDSavedQueryStartRequestV4 build();
    }

    public static TDSavedQueryStartRequestV4.Builder builder()
    {
        return ImmutableTDSavedQueryStartRequestV4.builder();
    }
}
