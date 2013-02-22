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
package com.treasure_data.model.bulkimport;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SessionSummary extends Session {
    public static enum Status {
        UPLOADING("uploading"),
        PERFORMING("performing"),
        READY("ready"),
        COMMITTING("committing"),
        COMMITTED("committed"),
        UNKNOWN("unknown");

        private String statusName;

        Status(String statusName) {
            this.statusName = statusName;
        }

        public String statusName() {
            return statusName;
        }

        public static Status fromString(String statusName) {
            return StringToStatus.get(statusName);
        }

        private static class StringToStatus {
            private static final Map<String, Status> REVERSE_DICTIONARY;

            static {
                Map<String, Status> map = new HashMap<String, Status>();
                for (Status elem : Status.values()) {
                    map.put(elem.statusName, elem);
                }
                REVERSE_DICTIONARY = Collections.unmodifiableMap(map);
            }

            static Status get(String key) {
                return REVERSE_DICTIONARY.get(key);
            }
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
                getName(), getDatabaseName(), getTableName(), status.statusName(),
                uploadFrozen, jobID, validRecords, errorRecords, validParts, errorParts);
    }

    public String getStatus() {
        return status.statusName();
    }

    public boolean uploadFrozen() {
        return uploadFrozen;
    }

    public String getJobID() {
        return jobID;
    }
}
