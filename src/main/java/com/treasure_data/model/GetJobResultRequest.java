package com.treasure_data.model;

public class GetJobResultRequest extends AbstractRequest<JobResult> {

    private JobResult result;

    public GetJobResultRequest(JobResult result) {
        this.result = result;
    }

    public Job getJob() {
        return result.getJob();
    }
}
