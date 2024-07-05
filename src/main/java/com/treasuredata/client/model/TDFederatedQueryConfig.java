package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;

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
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TDFederatedQueryConfig other = (TDFederatedQueryConfig) obj;
    return Objects.equal(this.id, other.id) &&
        Objects.equal(this.type, other.type) &&
        Objects.equal(this.userId, other.userId) &&
        Objects.equal(this.accountId, other.accountId) &&
        Objects.equal(this.name, other.name) &&
        Objects.equal(this.connectionId, other.connectionId) &&
        Objects.equal(this.createdAt, other.createdAt) &&
        Objects.equal(this.updatedAt, other.updatedAt) &&
        Objects.equal(this.settings, other.settings);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(id, type, userId, accountId, name, connectionId, createdAt, updatedAt, settings);
  }

  @Override
  public String toString()
  {
    return "TDFederatedQueryConfig{" +
        "id=" + id +
        ", type='" + type + '\'' +
        ", userId=" + userId +
        ", accountId=" + accountId +
        ", name='" + name + '\'' +
        ", connectionId=" + connectionId +
        ", createdAt='" + createdAt + '\'' +
        ", updatedAt='" + updatedAt + '\'' +
        ", settings='" + settings + '\'' +
        '}';
  }
}
