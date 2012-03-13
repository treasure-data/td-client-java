package com.treasure_data.model;

public class JobResult extends AbstractModel {

    private Job job;

    private org.msgpack.type.Value result;

    public JobResult(String jobID) {
        this(new Job(jobID));
    }

    public JobResult(Job job) {
        this(job, null);
    }

    public JobResult(Job job, org.msgpack.type.Value result) {
        super(job.getJobID());
        this.job = job;
        this.result = result;
    }

    public Job getJob() {
        return job;
    }

    public org.msgpack.type.Value getResult() {
        return result;
    }
}
