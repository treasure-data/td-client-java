package com.treasure_data.model;

import com.treasure_data.model.Table;
import com.treasure_data.model.TableSpecifyRequest;

public class SwapTableRequest extends TableSpecifyRequest<Table> {

    private String databaseName;

    private String tableName1;

    private String tableName2;

    public SwapTableRequest(String databaseName, String tableName1, String tableName2) {
        super(null);
        this.databaseName = databaseName;
        this.tableName1 = tableName1;
        this.tableName2 = tableName2;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName1() {
        return tableName1;
    }

    public String getTableName2() {
        return tableName2;
    }
}
