package com.treasure_data.model;

public class SubmitJobResult extends AbstractResult<Job> {

    public SubmitJobResult(Job job) {
        super(job);
    }

    public Job getJob() {
        return get();
    }
}
