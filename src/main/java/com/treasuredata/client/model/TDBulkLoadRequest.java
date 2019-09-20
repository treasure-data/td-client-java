package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TDBulkLoadRequest
{
    private String connectionId;
    private String connectionSettings;
    private String tableId;

    @JsonCreator
    TDBulkLoadRequest(
            @JsonProperty("connection_id") String connectionId,
            @JsonProperty("connection_settings") String connectionSettings,
            @JsonProperty("table_id") String tableId
            )
    {
        this.connectionId = connectionId;
        this.connectionSettings = connectionSettings;
        this.tableId = tableId;
    }

    TDBulkLoadRequest(TDBulkLoadRequestBuilder builder)
    {
        this.connectionId = builder.getConnectionId();
        this.connectionSettings = builder.getConnectionSettings();
        this.tableId = builder.getTableId();
    }

    @JsonProperty("connection_id")
    public String getConnectionId()
    {
        return connectionId;
    }

    @JsonProperty("connection_settings")
    public String getConnectionSettings()
    {
        return connectionSettings;
    }

    @JsonProperty("table_id")
    public String getTableId()
    {
        return tableId;
    }

    @Override
    public String toString()
    {
        return "TDBulkLoadRequest{" +
                "connectionId='" + connectionId + '\'' +
                ", connectionSettings='" + connectionSettings + '\'' +
                ", tableId='" + tableId + '\'' +
                '}';
    }

    public static TDBulkLoadRequestBuilder builder()
    {
        return new TDBulkLoadRequestBuilder();
    }

    public static TDBulkLoadRequest of()
    {
        return builder().build();
    }
}
