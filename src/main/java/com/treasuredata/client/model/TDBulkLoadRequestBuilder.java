package com.treasuredata.client.model;

public class TDBulkLoadRequestBuilder
{
    private String connectionId;
    private String connectionSettings;
    private String tableId;

    TDBulkLoadRequestBuilder()
    {
    }

    public String getConnectionId()
    {
        return connectionId;
    }

    public TDBulkLoadRequestBuilder setConnectionId(String connectionId)
    {
        this.connectionId = connectionId;
        return this;
    }

    public String getConnectionSettings()
    {
        return connectionSettings;
    }

    public TDBulkLoadRequestBuilder setConnectionSettings(String connectionSettings)
    {
        this.connectionSettings = connectionSettings;
        return this;
    }

    public String getTableId()
    {
        return tableId;
    }

    public TDBulkLoadRequestBuilder setTableId(String tableId)
    {
        this.tableId = tableId;
        return this;
    }

    public TDBulkLoadRequest build()
    {
        return new TDBulkLoadRequest(this);
    }
}
