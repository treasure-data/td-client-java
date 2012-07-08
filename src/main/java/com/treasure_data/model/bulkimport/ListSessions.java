package com.treasure_data.model.bulkimport;

import java.util.List;

import com.treasure_data.model.AbstractListModels;

public class ListSessions<T extends Session> extends AbstractListModels<T> {

    public ListSessions(List<T> sessions) {
        super(sessions);
    }
}