package com.treasure_data.model;

public class ShowJobRequest extends AbstractRequest<Job> {

    private Job job;

    public ShowJobRequest(Job job) {
        this.job = job;
    }

    public Job getJob() {
        return job;
    }
}
