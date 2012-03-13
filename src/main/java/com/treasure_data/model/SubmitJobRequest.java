package com.treasure_data.model;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class SubmitJobRequest extends AbstractRequest<Job> {

    private Database database;

    private String query;

    private String resultTableName;

    private boolean wait;

    public SubmitJobRequest(Database database, String query, String resultTableName, boolean wait) {
        this.database = database;
        this.query = query;
        this.resultTableName = resultTableName;
        this.wait = wait;
    }

    public Database getDatabase() {
        return database;
    }

    public String getQuery() {
        return query;
    }

    public String getResultTableName() {
        return resultTableName;
    }

    public boolean isWait() {
        return wait;
    }
}
