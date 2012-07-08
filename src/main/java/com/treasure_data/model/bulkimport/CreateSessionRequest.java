package com.treasure_data.model.bulkimport;

public class CreateSessionRequest extends BulkImportSpecifyRequest<Session> {

    public CreateSessionRequest(String sessName, String databaseName, String tableName) {
        super(new Session(sessName, databaseName, tableName));
    }

    public Session getSession() {
        return get();
    }

    public String getSessionName() {
        return get().getName();
    }

    public String getDatabaseName() {
        return get().getDatabaseName();
    }

    public String getTableName() {
        return get().getTableName();
    }
}
