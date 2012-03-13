package com.treasure_data.model;

public class ListTablesRequest extends AbstractRequest<ListTables> {

    private Database database;

    public ListTablesRequest() {
        this(null);
    }

    public ListTablesRequest(Database database) {
        this.database = database;
    }

    public Database getDatabase() {
        return database;
    }

}
