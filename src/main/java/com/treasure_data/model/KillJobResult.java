package com.treasure_data.model;

public class KillJobResult extends AbstractResult<Job> {

    private String jobID;

    private String status;

    public KillJobResult(String jobID, String status) {
        super(null);
        this.jobID = jobID;
        this.status = status;
    }

    public String getJobID() {
        return jobID;
    }

    public String getStatus() {
        return status;
    }
}
