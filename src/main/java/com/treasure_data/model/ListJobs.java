package com.treasure_data.model;

import java.util.List;

public class ListJobs extends AbstractListModels<Job> {

    private long count;

    private long from;

    private long to;

    public ListJobs(long count, long from, long to, List<Job> jobs) {
        super(jobs);
        this.count = count;
        this.from = from;
        this.to = to;
    }
}
