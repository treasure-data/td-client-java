package com.treasure_data.model.bulkimport;

public class GetErrorRecordsResult extends BulkImportSpecifyResult<Session> {

    public GetErrorRecordsResult(Session sess) {
        super(sess);
    }

    public ErrorRecords getErrorRecords() {
        return new ErrorRecords();
    }
}
