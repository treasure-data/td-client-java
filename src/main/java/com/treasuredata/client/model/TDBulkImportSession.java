/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonCollectionRootName(value = "bulk_imports")
public class TDBulkImportSession
{
    public static enum ImportStatus
    {
        UPLOADING("uploading"),
        PERFORMING("performing"),
        READY("ready"),
        COMMITTING("committing"),
        COMMITTED("committed"),
        UNKNOWN("unknown");
        private final String name;

        private ImportStatus(String name)
        {
            this.name = name;
        }

        @JsonCreator
        public static ImportStatus forName(String name)
        {
            return ImportStatus.valueOf(name.toUpperCase());
        }
    }

    private final String name;
    private final String databaseName;
    private final String tableName;
    private final ImportStatus status;
    private final boolean uploadFrozen;
    private final String jobId; // nullable
    private final long validRecords;
    private final long errorRecords;
    private final long validParts;
    private final long errorParts;

    @JsonCreator
    public TDBulkImportSession(
            @JsonProperty("name") String name,
            @JsonProperty("database") String databaseName,
            @JsonProperty("table") String tableName,
            @JsonProperty("status") ImportStatus status,
            @JsonProperty("upload_frozen") boolean uploadFrozen,
            @JsonProperty("job_id") String jobId,
            @JsonProperty("valid_records") long validRecords,
            @JsonProperty("error_records") long errorRecords,
            @JsonProperty("valid_parts") long validParts,
            @JsonProperty("error_parts") long errorParts)
    {
        this.name = name;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.status = status;
        this.uploadFrozen = uploadFrozen;
        this.jobId = jobId;
        this.validRecords = validRecords;
        this.errorRecords = errorRecords;
        this.validParts = validParts;
        this.errorParts = errorParts;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty("database")
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty("table")
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public ImportStatus getStatus()
    {
        return status;
    }

    @JsonProperty("upload_frozen")
    public boolean isUploadFrozen()
    {
        return uploadFrozen;
    }

    @JsonProperty("job_id")
    public String getJobId()
    {
        return jobId;
    }

    @JsonProperty("valid_records")
    public long getValidRecords()
    {
        return validRecords;
    }

    @JsonProperty("error_records")
    public long getErrorRecords()
    {
        return errorRecords;
    }

    @JsonProperty("valid_parts")
    public long getValidParts()
    {
        return validParts;
    }

    @JsonProperty("error_parts")
    public long getErrorParts()
    {
        return errorParts;
    }

    public boolean isUploading()
    {
        return status == ImportStatus.UPLOADING;
    }

    public boolean hasErrorOnPerform()
    {
        return validRecords == 0 || errorParts > 0 || errorRecords > 0;
    }

    public String getErrorMessage()
    {
        if (validRecords == 0) {
            return "No record processed";
        }
        if (errorRecords > 0) {
            return String.format("%d invalid parts", errorParts);
        }
        if (errorRecords > 0) {
            return String.format("%d invalid records", errorRecords);
        }

        return null;
    }
}
