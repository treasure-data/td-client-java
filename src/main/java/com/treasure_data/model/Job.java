//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
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
