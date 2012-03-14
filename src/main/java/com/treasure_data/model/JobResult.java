package com.treasure_data.model;

public class JobResult extends AbstractModel {

    public static enum Format {
        MSGPACK, JSON, UNKNOWN,
    }

    public static Format toFormat(String formatName) {
        if (formatName.equals("msgpack")) {
            return Format.MSGPACK;
        } else if (formatName.equals("json")) {
            return Format.JSON;
        } else {
            return Format.UNKNOWN;
        }
    }

    public static String toFormatName(Format format) {
        switch (format) {
        case MSGPACK:
            return "msgpack";
        case JSON:
            return "json";
        default:
            return "unknown";
        }
    }

    private Job job;

    private org.msgpack.type.Value result;

    private Format format;

    public JobResult(String jobID) {
        this(new Job(jobID), Format.MSGPACK);
    }

    public JobResult(Job job, Format format) {
        this(job, format, null);
    }

    private JobResult(Job job, Format format, org.msgpack.type.Value result) {
        super(job.getJobID());
        this.job = job;
        this.format = format;
        this.result = result;
    }

    public Job getJob() {
        return job;
    }

    public void setResult(org.msgpack.type.Value result) {
        this.result = result;
    }

    public org.msgpack.type.Value getResult() {
        return result;
    }

    public Format getFormat() {
        return format;
    }
}
