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
package com.treasure_data.model.bulkimport;

public class SessionSummary extends Session {
    public static enum Status {
        UPLOADING, PERFORMING, READY, COMMITTING, COMMITTED, UNKNOWN;
    }

    public static Status toStatus(String statusName) {
        if (statusName == null) {
            throw new NullPointerException();
        }

        if (statusName.equals("uploading"))
            return Status.UPLOADING;
        if (statusName.equals("performing"))
            return Status.PERFORMING;
        if (statusName.equals("ready")) {
            return Status.READY;
        } else if (statusName.equals("committing")) {
            return Status.COMMITTING;
        } else if (statusName.equals("committed")) {
            return Status.COMMITTED;
        } else {
            return Status.UNKNOWN;
        }
    }

    public static String toStatusName(Status status) {
        if (status == null) {
            throw new NullPointerException();
        }

        switch (status) {
        case UPLOADING:
            return "uploading";
        case PERFORMING:
            return "performing";
        case READY:
            return "ready";
        case COMMITTING:
            return "performing";
        case COMMITTED:
            return "committing";
        default:
            return "unknown";
        }
    }

    private Status status;

    private boolean uploadFrozen;

    private String jobID; // nullable

    private long validRecords;

    private long errorRecords;

    private long validParts;

    private long errorParts;

    public SessionSummary(String name, String databaseName, String tableName,
            Status status, boolean uploadFrozen, String jobID,
            long validRecords, long errorRecords,
            long validParts, long errorParts) {
        super(name, databaseName, tableName);
        this.status = status;
        this.uploadFrozen = uploadFrozen;
        this.jobID = jobID;
        this.validRecords = validRecords;
        this.errorRecords = errorRecords;
        this.validParts = validParts;
        this.errorParts = errorParts;
    }

    @Override
    public String toString() {
        return String.format("SessionSummary{name=%s, db=%s, tbl=%s, frozen=%b, stat=%s, jid=%s, vr=%d, er=%d, vp=%d, ep=%d}",
                getName(), getDatabaseName(), getTableName(), toStatusName(status),
                uploadFrozen, jobID, validRecords, errorRecords, validParts, errorParts);
    }

    public String getStatus() {
        return toStatusName(status);
    }

    public boolean uploadFrozen() {
        return uploadFrozen;
    }

    public String getJobID() {
        return jobID;
    }
}
