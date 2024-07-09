package com.treasuredata.client.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class TDFederatedQueryConfig
{
    private final int id;
    private final String type;
    private final int userId;
    private final int accountId;
    private final String name;
    private final int connectionId;
    private final String createdAt;
    private final String updatedAt;

    @JsonDeserialize(using = com.treasuredata.client.deserialize.FederatedQueryConfigSettingsDeserializer.class)
    private final String settings;

    @JsonCreator
    public TDFederatedQueryConfig(
            @JsonProperty("id") int id,
            @JsonProperty("type") String type,
            @JsonProperty("user_id") int userId,
            @JsonProperty("account_id") int accountId,
            @JsonProperty("name") String name,
            @JsonProperty("connection_id") int connectionId,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt,
            @JsonProperty("settings") String settings)
    {
        this.id = id;
        this.type = type;
        this.userId = userId;
        this.accountId = accountId;
        this.name = name;
        this.connectionId = connectionId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.settings = settings;
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty("user_id")
    public int getUserId()
    {
        return userId;
    }

    @JsonProperty("account_id")
    public int getAccountId()
    {
        return accountId;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty("connection_id")
    public int getConnectionId()
    {
        return connectionId;
    }

    @JsonProperty("created_at")
    public String getCreatedAt()
    {
        return createdAt;
    }

    @JsonProperty("updated_at")
    public String getUpdatedAt()
    {
        return updatedAt;
    }

    @JsonProperty
    public String getSettings()
    {
        return settings;
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TDFederatedQueryConfig)) {
            return false;
        }
        TDFederatedQueryConfig that = (TDFederatedQueryConfig) o;
        return id == that.id
                && userId == that.userId
                && accountId == that.accountId
                && connectionId == that.connectionId
                && Objects.equals(type, that.type)
                && Objects.equals(name, that.name)
                && Objects.equals(createdAt, that.createdAt)
                && Objects.equals(updatedAt, that.updatedAt)
                && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode()
    {
        int result = id;
        result = 31 * result + Objects.hashCode(type);
        result = 31 * result + userId;
        result = 31 * result + accountId;
        result = 31 * result + Objects.hashCode(name);
        result = 31 * result + connectionId;
        result = 31 * result + Objects.hashCode(createdAt);
        result = 31 * result + Objects.hashCode(updatedAt);
        result = 31 * result + Objects.hashCode(settings);
        return result;
    }

    @Override
    public String toString()
    {
        return "TDFederatedQueryConfig{"
                + "id="
                + id
                + ", type='"
                + type
                + '\''
                + ", userId="
                + userId
                + ", accountId="
                + accountId
                + ", name='"
                + name
                + '\''
                + ", connectionId="
                + connectionId
                + ", createdAt='"
                + createdAt
                + '\''
                + ", updatedAt='"
                + updatedAt
                + '\''
                + ", settings='"
                + settings
                + '\''
                + '}';
    }
}
