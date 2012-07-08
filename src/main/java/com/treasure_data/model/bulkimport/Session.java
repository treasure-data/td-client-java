package com.treasure_data.model.bulkimport;

import java.util.List;

import com.treasure_data.model.AbstractModel;

public class Session extends AbstractModel {

    private String databaseName;

    private String tableName;

    private List<String> files;

    public Session(String name, String databaseName, String tableName) {
        super(name);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getUploadedFiles() {
        return files;
    }
}
