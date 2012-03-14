package com.treasure_data.model;

import java.util.List;

public class ListJobsResult extends AbstractResult<ListJobs> {

    public ListJobsResult(ListJobs jobs) {
        super(jobs);
    }

    public List<Job> getJobs() {
        return get().get();
    }
}
