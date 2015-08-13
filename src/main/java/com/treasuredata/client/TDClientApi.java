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

import com.treasuredata.client.api.model.TDJob;
import com.treasuredata.client.api.model.TDJobList;
import com.treasuredata.client.api.model.TDJobRequest;
import com.treasuredata.client.api.model.TDJobResult;
import com.treasuredata.client.api.model.TDJobSubmitResult;
import com.treasuredata.client.api.model.TDTable;

import java.util.List;

/**
 * Treasure Data Client
 */
public interface TDClientApi
{
    /**
     * Get the list of databases
     * @return list of databases
     * @throws TDClientException if failed to retrieve the database list
     */
    List<String> listDatabases() throws TDClientException;

    /**
     * Create a new database
     * @param databaseName
     * @throws TDClientException if the specified database already exists
     */
    void createDatabase(String databaseName) throws TDClientException;

    void createDatabaseIfNotExists(String databaseName) throws TDClientException;

    /**
     * Delete a specified database. Deleting a database deletes all of its belonging tables.
     * @param databaseName
     * @throws TDClientException if no such a database exists
     */
    void deleteDatabase(String databaseName) throws TDClientException;

    void deleteDatabaseIfExists(String databaseName) throws TDClientException;

    /**
     * Get the list of the tables in the specified database
     * @param databaseName
     * @return
     * @throws TDClientException
     */
    List<TDTable> listTables(String databaseName) throws TDClientException;

    boolean existsDatabase(String databaseName) throws TDClientException;

    boolean existsTable(String databaseName, String table) throws TDClientException;

    /**
     * Create a new table
     * @param databaseName
     * @param tableName
     * @return
     * @throws TDClientException
     */
    void createTable(String databaseName, String tableName) throws TDClientException;

    void createTableIfNotExists(String databaseName, String tableName) throws TDClientException;

    void renameTable(String databaseName, String tableName, String newTableName) throws TDClientException;

    void renameTable(String databaseName, String tableName, String newTableName, boolean overwrite) throws TDClientException;

    void deleteTable(String databaseName, String tableName) throws TDClientException;

    void deleteTableIfExists(String databaseName, String tableName) throws TDClientException;

    void partialDelete(String databaseName, String tableName, long from, long to) throws TDClientException;

    /**
     * Submit a new job request
     * @param jobRequest
     * @return job_id
     * @throws TDClientException
     */
    String submit(TDJobRequest jobRequest) throws TDClientException;

    TDJobList listJobs() throws TDClientException;

    TDJobList listJobs(long from, long to) throws TDClientException;

    void killJob(String jobId) throws TDClientException;

    TDJob jobStatus(String jobId) throws TDClientException;

    TDJob jobInfo(String jobId) throws TDClientException;

    TDJobResult jobResult(String jobId) throws TDClientException;

    //

}
