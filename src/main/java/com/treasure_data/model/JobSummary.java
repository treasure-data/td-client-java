//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.model;

public class JobSummary extends Job {

    public static enum Status {
        QUEUED, BOOTING, RUNNING, SUCCESS, ERROR, KILLED, UNKNOWN,
    }

    public static Status toStatus(String statusName) {
        if (statusName == null) {
            throw new NullPointerException();
        }

        if (statusName.equals("queued")) {
            return Status.QUEUED;
        } else if (statusName.equals("booting")) {
            return Status.BOOTING;
        } else if (statusName.equals("running")) {
            return Status.RUNNING;
        } else if (statusName.equals("success")) {
            return Status.SUCCESS;
        } else if (statusName.equals("error")) {
            return Status.ERROR;
        } else if (statusName.equals("killed")) {
            return Status.KILLED;
        } else {
            return Status.UNKNOWN;
        }
    }

    public static String toStatusName(Status status) {
        if (status == null) {
            throw new NullPointerException();
        }

        switch (status) {
        case QUEUED:
            return "queued";
        case BOOTING:
            return "booting";
        case RUNNING:
            return "running";
        case SUCCESS:
            return "success";
        case ERROR:
            return "error";
        case KILLED:
            return "killed";
        default:
            return "unknown";
        }
    }

    public static class Debug {
        private String cmdout;
        private String stderr;

        public Debug(String cmdout, String stderr) {
            this.cmdout = cmdout;
            this.stderr = stderr;
        }

        public String getCmdout() {
            return cmdout;
        }

        public String getStderr() {
            return stderr;
        }
    }

    private Status status;

    private String createdAt;

    private String startAt;

    private String endAt;

    private String resultSchema; // hive-specific schema

    private Debug debug;

    public JobSummary(String jobID, JobSummary.Type type, Database database,
            String url, String resultTable, Status status, String startAt, String endAt,
            String query, String resultSchema) {
        this(jobID, type, database, url, resultTable, status, startAt, endAt,
                query, resultSchema, null);
    }

    public JobSummary(String jobID, JobSummary.Type type, Database database,
            String url, String resultTable, Status status, String startAt, String endAt,
            String query, String resultSchema, Debug debug) {
        super(jobID, type, database, url, resultTable);
        this.status = status;
        this.startAt = startAt;
        this.endAt = endAt;
        setQuery(query);
        this.resultSchema = resultSchema;
        this.debug = debug;
    }

    public void setStatus(JobSummary.Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public void setCreatedAt(String time) {
        this.createdAt = time;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setStartAt(String time) {
        this.startAt = time;
    }

    public String getStartAt() {
        return startAt;
    }

    public void setEndAt(String time) {
        this.endAt = time;
    }

    public String getEndAt() {
        return endAt;
    }

    public String getResultSchema() {
        return resultSchema;
    }

    public Debug getDebug() {
        return debug;
    }
}
