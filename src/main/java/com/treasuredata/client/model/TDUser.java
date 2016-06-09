package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableTDUser.class)
@JsonDeserialize(as = ImmutableTDUser.class)
public abstract class TDUser
{
    @JsonSerialize(using = StringToNumberSerializer.class) @JsonProperty("id") public abstract String getId();
    @JsonProperty("name") public abstract String getName();
    @JsonProperty("first_name") public abstract String getFirstName();
    @JsonProperty("last_name") public abstract String getLastName();
    @JsonProperty("email") public abstract String getEmail();
    @JsonProperty("phone") public abstract String getPhone();
    @JsonProperty("gravatar_url") public abstract String getGravatarUrl();
    @JsonProperty("administrator") public abstract boolean isAdministrator();
    @JsonProperty("created_at") public abstract String getCreatedAt();
    @JsonProperty("updated_at") public abstract String getUpdatedAt();
    @JsonProperty("account_owner") public abstract boolean isAccountOwner();

    public interface Builder
    {
        Builder id(String id);
        Builder name(String name);
        Builder firstName(String firstName);
        Builder lastName(String lastName);
        Builder email(String email);
        Builder phone(String phone);
        Builder gravatarUrl(String gravatarUrl);
        Builder isAdministrator(boolean administrator);
        Builder createdAt(String createdAt);
        Builder updatedAt(String updatedAt);
        Builder isAccountOwner(boolean accountOwner);
        TDUser build();
    }

    public static Builder builder()
    {
        return ImmutableTDUser.builder();
    }
}
