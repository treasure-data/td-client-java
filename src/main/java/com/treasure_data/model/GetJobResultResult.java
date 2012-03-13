package com.treasure_data.model;

public class GetJobResultResult extends AbstractResult<JobResult> {

    public GetJobResultResult(JobResult result) {
        super(result);
    }

    public Job getJob() {
        return get().getJob();
    }

    public org.msgpack.type.Value getResult() {
        return get().getResult();
    }
}
