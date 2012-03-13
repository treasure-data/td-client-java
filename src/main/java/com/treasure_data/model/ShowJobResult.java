package com.treasure_data.model;

public class ShowJobResult extends AbstractResult<Job> {

    public ShowJobResult(Job job) {
        super(job);
    }

    public Job getJob() {
        return get();
    }
}
