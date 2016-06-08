package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.auto.value.AutoValue;

@AutoValue
@JsonDeserialize(builder = AutoValue_TDUser.Builder.class)
public abstract class TDUser
{
    @JsonProperty("id") public abstract String getId();
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

    @AutoValue.Builder
    public static abstract class Builder {
        @JsonProperty("id") public abstract Builder id(String id);
        @JsonProperty("name") public abstract Builder name(String name);
        @JsonProperty("first_name") public abstract Builder firstName(String firstName);
        @JsonProperty("last_name") public abstract Builder lastName(String lastName);
        @JsonProperty("email") public abstract Builder email(String email);
        @JsonProperty("phone") public abstract Builder phone(String phone);
        @JsonProperty("gravatar_url") public abstract Builder gravatarUrl(String gravatarUrl);
        @JsonProperty("administrator") public abstract Builder administrator(boolean administrator);
        @JsonProperty("created_at") public abstract Builder createdAt(String createdAt);
        @JsonProperty("updated_at") public abstract Builder updatedAt(String updatedAt);
        @JsonProperty("account_owner") public abstract Builder accountOwner(boolean accountOwner);
        public abstract TDUser build();
    }

    public static Builder builder() {
        return new AutoValue_TDUser.Builder();
    }
}

