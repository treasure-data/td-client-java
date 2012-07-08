package com.treasure_data.model.bulkimport;

import java.util.List;

public class ListFilesResult extends BulkImportSpecifyResult<Session> {

    public ListFilesResult(Session sess) {
        super(sess);
    }

    public List<String> getUploadedFiles() {
        return get().getUploadedFiles();
    }
}
