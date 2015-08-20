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
package com.treasuredata.client;

import com.treasuredata.client.model.TDAuthenticationResult;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobStatus;
import com.treasuredata.client.model.TDTable;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * Treasure Data Client
 */
public interface TDClientApi<TDClientImpl>
{
    String serverStatus();


    /**
     * Get the list of databases
     *
     * @return list of databases
     * @throws TDClientException if failed to retrieve the database list
     */
    List<String> listDatabases();

    /**
     * Create a new database
     *
     * @param databaseName
     * @throws TDClientException if the specified database already exists
     */
    void createDatabase(String databaseName);

    void createDatabaseIfNotExists(String databaseName);

    /**
     * Delete a specified database. Deleting a database deletes all of its belonging tables.
     *
     * @param databaseName
     * @throws TDClientException if no such a database exists
     */
    void deleteDatabase(String databaseName);

    void deleteDatabaseIfExists(String databaseName);

    /**
     * Get the list of the tables in the specified database
     *
     * @param databaseName
     * @return
     * @throws TDClientException
     */
    List<TDTable> listTables(String databaseName);

    boolean existsDatabase(String databaseName);

    boolean existsTable(String databaseName, String table);

    /**
     * Create a new table
     *
     * @param databaseName
     * @param tableName
     * @return
     * @throws TDClientException
     */
    void createTable(String databaseName, String tableName);

    void createTableIfNotExists(String databaseName, String tableName);

    void renameTable(String databaseName, String tableName, String newTableName);

    void renameTable(String databaseName, String tableName, String newTableName, boolean overwrite);

    void deleteTable(String databaseName, String tableName);

    void deleteTableIfExists(String databaseName, String tableName);

    void partialDelete(String databaseName, String tableName, long from, long to);

    /**
     * Submit a new job request
     *
     * @param jobRequest
     * @return job_id
     * @throws TDClientException
     */
    String submit(TDJobRequest jobRequest);

    TDJobList listJobs();

    TDJobList listJobs(long from, long to);

    void killJob(String jobId);

    TDJobStatus jobStatus(String jobId);

    TDJob jobInfo(String jobId);

    /**
     * Open an input stream to retrieve the job result.
     * This method does not close the returned InputStream.
     *
     * You will receive an empty stream if the query has not finished yet.
     * @param jobId
     * @param format
     * @return
     */
    InputStream jobResult(String jobId, TDResultFormat format);

    // bulk import API
    void createBulkImportSession(String sessionName, String databaseName, String tableName);
    TDBulkImportSession getBulkImportSession(String sessionName);
    void uploadBulkImportPart(String sessionName, String uniquePartName, File path);
    void freezeBulkImportSession(String sessionName);
    void performBulkImportSession(String sessionName, int priority);
    void commitBulkImportSession(String sessionName);
    void deleteBulkImportSession(String sessionName);

    /**
     * Perform user email and passsword based authentication.
     * @param email
     * @param password
     * @return
     */
    TDAuthenticationResult authenticate(String email, String password);
}