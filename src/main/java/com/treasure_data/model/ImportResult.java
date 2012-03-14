package com.treasure_data.model;

public class ImportResult extends AbstractResult<Table> {

    public ImportResult(Table table) {
        super(table);
    }

    public Table getTable() {
        return this.get();
    }
}
