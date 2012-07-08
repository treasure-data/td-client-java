package com.treasure_data.model.bulkimport;

import java.util.List;

import com.treasure_data.model.AbstractResult;

public class ListSessionsResult extends AbstractResult<ListSessions<SessionSummary>> {
    // TODO #MN T is Session? not SessionSummary?

    public ListSessionsResult(ListSessions<SessionSummary> sessions) {
        super(sessions);
    }

    public List<SessionSummary> getSessions() {
        return get().get();
    }

    public Session getSession(String name) {
        return get().get(name);
    }
}
