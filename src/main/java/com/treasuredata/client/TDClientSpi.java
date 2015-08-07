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

import com.treasuredata.client.api.model.TDDatabase;
import com.treasuredata.client.api.model.TDJob;
import com.treasuredata.client.api.model.TDJobRequest;
import com.treasuredata.client.api.model.TDJobResult;
import com.treasuredata.client.api.model.TDTable;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Treasure Data Client
 */
public interface TDClientSpi
{
    final String TD_API_LIST_DATABASES = "/v3/database/list";
    final String TD_API_LIST_TABLES = "/v3/table/list";


    /**
     * Get the list of databases
     * @return list of databases
     * @throws TDClientException if failed to retrieve the database list
     */
    List<String> listDatabases() throws TDClientException;

    /**
     * Create a new database
     * @param databaseName
     * @return
     * @throws TDClientException if the specified database already exists
     */
    boolean createDatabase(String databaseName) throws TDClientException;

    /**
     * Delete a specified database. Deleting a database deletes all of its belonging tables.
     * @param databaseName
     * @return success or not
     * @throws TDClientException
     */
    void deleteDatabase(String databaseName) throws TDClientException;

    /**
     * Get the list of the tables in the specified database
     * @param databaseName
     * @return
     * @throws TDClientException
     */
    List<TDTable> listTables(String databaseName) throws TDClientException;

    /**
     * Get the list of tables in the specified database
     * @param database
     * @return
     * @throws TDClientException
     */
    List<TDTable> listTables(TDDatabase database) throws TDClientException;

    /**
     * Create a new table
     * @param databaseName
     * @param tableName
     * @return
     * @throws TDClientException
     */
    TDTable createTable(String databaseName, String tableName) throws TDClientException;

    TDTable createTable(String databaseName, TDTable table) throws TDClientException;

    void renameTable(String databaseName, String tableName, String newTableName) throws TDClientException;

    void renameTable(TDTable table, String newTableName) throws TDClientException;

    void deleteTable(String databasename, String tableName) throws TDClientException;

    void deleteTable(TDTable table) throws TDClientException;

    void partialDelete(TDTable table, long from, long to) throws TDClientException;

    Future<TDJobResult> submit(TDJobRequest jobRequest) throws TDClientException;

    List<TDJob> listJobs() throws TDClientException;

    List<TDJob> listJobs(long from, long to) throws TDClientException;

    void killJob(String jobId) throws TDClientException;

    TDJob jobStatus(String jobId) throws TDClientException;

    TDJobResult jobResult(String jobId) throws TDClientException;

    //

}
