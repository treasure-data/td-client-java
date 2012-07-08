package com.treasure_data.model.bulkimport;

import com.treasure_data.model.AbstractRequest;

public class BulkImportSpecifyRequest<T extends Session> extends AbstractRequest<T>{

    protected BulkImportSpecifyRequest(T sess) {
        super(sess);
    }

    public T getSession() {
        return get();
    }

    public String getSessionName() {
        return get().getName();
    }
}
