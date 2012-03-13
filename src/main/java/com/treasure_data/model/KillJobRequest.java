package com.treasure_data.model;

public class KillJobRequest extends AbstractRequest<Job> {

    private Job job;

    public KillJobRequest(Job job) {
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

}
