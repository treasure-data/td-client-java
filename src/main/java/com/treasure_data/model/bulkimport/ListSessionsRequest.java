package com.treasure_data.model.bulkimport;

import com.treasure_data.model.AbstractRequest;

public class ListSessionsRequest extends AbstractRequest<ListSessions<SessionSummary>> {
    // TODO #MN T is Session? not SessionSummary?

    public ListSessionsRequest() {
        super(null);
    }

}
