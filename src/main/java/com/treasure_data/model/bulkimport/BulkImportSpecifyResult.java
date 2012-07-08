package com.treasure_data.model.bulkimport;

import com.treasure_data.model.AbstractResult;

public class BulkImportSpecifyResult<T extends Session> extends AbstractResult<T> {

    protected BulkImportSpecifyResult(T sess) {
        super(sess);
    }

    public T getSession() {
        return get();
    }

    public String getSessionName() {
        return get().getName();
    }
}
