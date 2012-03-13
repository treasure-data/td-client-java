package com.treasure_data.model;

public class ListJobsRequest extends AbstractRequest<Job> {
    private long from;

    private long to;

    public ListJobsRequest() {
        this(0, 0);
    }

    public ListJobsRequest(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }
}
