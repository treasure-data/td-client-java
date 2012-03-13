package com.treasure_data.model;

public class Job extends AbstractModel {

    public static enum Status {
        RUNNING, SUCCESS, ERROR, UNKNOWN,
        // TODO #MN
    }

    public static Status toStatus(String statusName) {
        if (statusName == null) {
            throw new NullPointerException();
        }

        if (statusName.equals("running")) {
            return Status.RUNNING;
        } else if (statusName.equals("success")) {
            return Status.SUCCESS;
        } else if (statusName.equals("error")) {
            return Status.ERROR;
        } else {
            return Status.UNKNOWN;
        }
        // TODO #MN
    }

    public static String toStatusName(Status status) {
        switch (status) {
        case RUNNING:
            return "running";
        case SUCCESS:
            return "success";
        case ERROR:
            return "error";
        default:
            return "unknown";
        }
        // TODO #MN
    }

    private String jobID;

    private String type;

    private Database database;

    private String url;

    private Status status;

    private String startAt;

    private String endAt;

    private String query;

    private String result;

    public Job(String jobID) {
        super(jobID);
    }

    public Job(String jobID, String type) {
        super(jobID);
        this.type = type;
    }

    public Job(String jobID, String type, Database database, String url) {
        super(jobID);
        this.type = type;
        this.database = database;
        this.url = url;
    }

    public Job(String jobID, String type, Status status, String startAt,
            String endAt, String query, String result) {
        super(jobID);
        this.type = type;
        this.status = status;
        this.startAt = startAt;
        this.endAt = endAt;
        this.query = query;
        this.result = result;
    }

    public String getJobID() {
        return getName();
    }

    public String getType() {
        return type;
    }
    public Database getDatabase() {
        return database;
    }

    public String getURL() {
        return url;
    }

    public Status getStatus() {
        return status;
    }
}
