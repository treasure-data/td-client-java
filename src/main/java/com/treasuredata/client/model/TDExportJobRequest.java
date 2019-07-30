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

import org.immutables.builder.Builder;
import org.immutables.value.Value;

import java.util.Date;
import java.util.Optional;

/**
 *
 */
@Value.Style(typeBuilder = "TDExportJobRequestBuilder")
public class TDExportJobRequest
{
    private final String database;
    private final String table;
    private final Date from;
    private final Date to;
    private final TDExportFileFormatType fileFormat;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String bucketName;
    private final String filePrefix;
    private final Optional<String> poolName;
    private final Optional<String> domainKey;

    /**
     * @deprecated Use {@link #builder()} instead.
     */
    @Deprecated
    public TDExportJobRequest(
            String database,
            String table,
            Date from,
            Date to,
            TDExportFileFormatType fileFormat,
            String accessKeyId,
            String secretAccessKey,
            String bucketName,
            String filePrefix,
            Optional<String> poolName)
    {
        this.database = database;
        this.table = table;
        this.from = (Date) from.clone();
        this.to = (Date) to.clone();
        this.fileFormat = fileFormat;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.bucketName = bucketName;
        this.filePrefix = filePrefix;
        this.poolName = poolName;
        this.domainKey = Optional.empty();
    }

    private TDExportJobRequest(String database, String table, Date from, Date to, TDExportFileFormatType fileFormat, String accessKeyId, String secretAccessKey, String bucketName, String filePrefix, Optional<String> poolName, Optional<String> domainKey)
    {
        this.database = database;
        this.table = table;
        this.from = from;
        this.to = to;
        this.fileFormat = fileFormat;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.bucketName = bucketName;
        this.filePrefix = filePrefix;
        this.poolName = poolName;
        this.domainKey = domainKey;
    }

    public String getDatabase()
    {
        return database;
    }

    public String getTable()
    {
        return table;
    }

    public Date getFrom()
    {
        // Date object is mutable
        return new Date(from.getTime());
    }

    public Date getTo()
    {
        // Date object is mutable
        return new Date(to.getTime());
    }

    public TDExportFileFormatType getFileFormat()
    {
        return fileFormat;
    }

    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    public String getFilePrefix()
    {
        return filePrefix;
    }

    public Optional<String> getPoolName()
    {
        return poolName;
    }

    public Optional<String> getDomainKey()
    {
        return domainKey;
    }

    @Builder.Factory
    static TDExportJobRequest of(String database, String table, Date from, Date to, TDExportFileFormatType fileFormat, String accessKeyId, String secretAccessKey, String bucketName, String filePrefix, Optional<String> poolName, Optional<String> domainKey)
    {
        return new TDExportJobRequest(database, table, from, to, fileFormat, accessKeyId, secretAccessKey, bucketName, filePrefix, poolName, domainKey);
    }

    public static TDExportJobRequestBuilder builder()
    {
        return new TDExportJobRequestBuilder();
    }

    @Override
    public String toString()
    {
        return "TDExportJobRequest{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", from=" + (from.getTime() / 1000) +
                ", to=" + (to.getTime() / 1000) +
                ", fileFormat='" + fileFormat + '\'' +
                ", accessKeyId='" + accessKeyId + '\'' +
                ", bucketName='" + bucketName + '\'' +
                ", filePrefix='" + filePrefix + '\'' +
                '}';
    }
}
