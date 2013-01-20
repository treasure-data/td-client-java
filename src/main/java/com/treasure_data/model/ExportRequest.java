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

public class ExportRequest extends TableSpecifyRequest<Table> {

    private String storageType;

    private String bucketName;

    private String fileFormat;

    private String accessKeyID;

    private String secretAccessKey;

    private String from;

    private String to;

    public ExportRequest(Table table, String storageType, String bucketName, String fileFormat,
            String accessKeyID, String secretAccessKey) {
        this(table, storageType, bucketName, fileFormat, accessKeyID, secretAccessKey, null, null);
    }

    public ExportRequest(Table table, String storageType, String bucketName, String fileFormat,
            String accessKeyID, String secretAccessKey, String from, String to) {
        super(table);
        this.storageType = storageType;
        this.bucketName = bucketName;
        this.fileFormat = fileFormat;
        this.accessKeyID = accessKeyID;
        this.secretAccessKey = secretAccessKey;
        this.from = from;
        this.to = to;
    }

    public String getStorageType() {
        return storageType;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public String getAccessKeyID() {
        return accessKeyID;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }
}
