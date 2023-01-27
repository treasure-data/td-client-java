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

import com.google.common.base.Function;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDColumnType;
import com.treasuredata.client.model.TDDatabase;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import com.treasuredata.client.model.TDSavedQueryHistory;
import com.treasuredata.client.model.TDSavedQueryUpdateRequest;
import com.treasuredata.client.model.TDTable;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ImmutableMapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class Example
{
    protected Example()
    {
    }

    public static void main(String[] args)
    {
        String dbName = "td_client_java_db";
        //String bulkName = "td_client_java_bulk_example";
        String table1 = "table1";
        //String table2 = "table2";

        // Create database
        //Example.createDatabaseExample(dbName);

        // Create tables
        //Example.createTableExample(dbName, table1);
        //Example.createTableExample(dbName, table2);

        // Update table schema
        List<TDColumn> columns = new ArrayList<>();
        TDColumn column = new TDColumn("col1", TDColumnType.STRING);
        columns.add(column);
        column = new TDColumn("col2", TDColumnType.STRING);
        columns.add(column);
        Example.updateSchemaExample(dbName, table1, columns);

        // Import data to table
        //Example.createDatabaseExample(dbName);
        //Example.createTableExample(dbName, table1);
        //Example.importDataExample(dbName, table1);

        // Bulk import
        //Example.createDatabaseExample(dbName);
        //Example.createTableExample(dbName, table1);
        //Example.updateSchemaExample(dbName, table1, columns);
        //Example.bulkImportExample(bulkName, dbName, table1);
    }

    public static void createDatabaseExample(String databaseName)
    {
        TDClient client = TDClient.newClient();

        try {
            client.createDatabase(databaseName);
            System.out.print("Database " + databaseName + " is created!");
        }
        catch (TDClientException e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void deleteDatabaseExample(String databaseName)
    {
        TDClient client = TDClient.newClient();

        try {
            client.deleteDatabase(databaseName);
            System.out.print("Database " + databaseName + " is deleted!");
        }
        catch (TDClientException e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void submitJobExample()
    {
        TDClient client = TDClient.newClient();
        try {
            // Submit a new Presto query
            String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "select count(1) cnt from www_access"));

            // Wait until the query finishes
            BackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(jobId);
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(jobId);
            }

            // Read the detailed job information
            TDJob jobInfo = client.jobInfo(jobId);
            System.out.println("log:\n" + jobInfo.getCmdOut());
            System.out.println("error log:\n" + jobInfo.getStdErr());

            // Read the job results in msgpack.gz format
            client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Integer>() {
                @Override
                public Integer apply(InputStream input)
                {
                    int count = 0;
                    try {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                        while (unpacker.hasNext()) {
                            // Each row of the query result is array type value (e.g., [1, "name", ...])
                            ArrayValue array = unpacker.unpackValue().asArrayValue();
                            System.out.println(array);
                            count++;
                        }
                        unpacker.close();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return count;
                }
            });
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void listDatabasesExample()
    {
        TDClient client = TDClient.newClient();
        try {
            // Retrieve database and table names
            List<TDDatabase> databases = client.listDatabases();
            TDDatabase db = databases.get(0);
            System.out.println("database: " + db.getName());
            for (TDTable table : client.listTables(db.getName())) {
                System.out.println(" table: " + table);
            }
        }
        catch (TDClientException e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void createTableExample(String databaseName, String tableName)
    {
        TDClient client = TDClient.newClient();
        try {
            client.createTable(databaseName, tableName);
            System.out.println("Table " + tableName + " is created in database " + databaseName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void deleteTableExample(String databaseName, String tableName)
    {
        TDClient client = TDClient.newClient();
        try {
            client.deleteTable(databaseName, tableName);
            System.out.println("Table " + tableName + " is deleted from database " + databaseName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void updateSchemaExample(String databaseName, String tableName, List<TDColumn> columns)
    {
        TDClient client = TDClient.newClient();
        try {
            client.updateTableSchema(databaseName, tableName, columns);
            System.out.println("Updated schema for table " + tableName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void importDataExample(String databaseName, String tableName)
    {
        TDClient client = TDClient.newClient();
        try {
            File file = File.createTempFile("data", ".msgpack.gz");
            System.out.println("File path: " + file.getAbsolutePath());
            file.deleteOnExit();

            StringValue timeCol = ValueFactory.newString("time");
            StringValue timeColValue = ValueFactory.newString("1");
            StringValue col1 = ValueFactory.newString("col1");
            StringValue col1Value = ValueFactory.newString("value1");
            StringValue col2 = ValueFactory.newString("col2");
            StringValue col2Value = ValueFactory.newString("value2");

            Map<Value, Value> sampleData = new HashMap<>();
            sampleData.put(timeCol, timeColValue);
            sampleData.put(col1, col1Value);
            sampleData.put(col2, col2Value);

            ImmutableMapValue mapValue = ValueFactory.newMap(sampleData);

            MessagePacker packer = MessagePack.newDefaultPacker(new GZIPOutputStream(new FileOutputStream(file)));
            packer.packValue(mapValue);
            packer.close();

            client.importFile(databaseName, tableName, file);
            System.out.println("Done importing data into table " + tableName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static File createBulkImportData() throws IOException
    {
        File file = File.createTempFile("data", ".msgpack.gz");
        file.deleteOnExit();

        Map<Value, Value> mapData = new HashMap<>();

        MessagePacker packer = MessagePack.newDefaultPacker(new GZIPOutputStream(new FileOutputStream(file)));
        int numberOfRecords = 100;
        for (int i = 1; i <= numberOfRecords; i++) {
            StringValue timeCol = ValueFactory.newString("time");
            StringValue timeColValue = ValueFactory.newString(i + "");
            StringValue col1 = ValueFactory.newString("col1");
            StringValue col1Value = ValueFactory.newString("value" + i);
            StringValue col2 = ValueFactory.newString("col2");
            StringValue col2Value = ValueFactory.newString("value2_" + i);

            mapData.put(timeCol, timeColValue);
            mapData.put(col1, col1Value);
            mapData.put(col2, col2Value);

            ImmutableMapValue mapValue = ValueFactory.newMap(mapData);
            packer.packValue(mapValue);
        }
        packer.close();

        return file;
    }

    public static void bulkImportExample(String bulkName, String databaseName, String tableName)
    {
        TDClient client = TDClient.newClient();
        try {
            File msgpackFile = Example.createBulkImportData();

            client.createBulkImportSession(bulkName, databaseName, tableName);
            client.uploadBulkImportPart(bulkName, "td_client_java_part_1", msgpackFile);
            client.performBulkImportSession(bulkName);

            TDBulkImportSession importSession = client.getBulkImportSession(bulkName);
            // Wait until the importing finishes
            ExponentialBackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(importSession.getJobId());
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(importSession.getJobId());
            }

            System.out.print("Bulk import job status: " + job.getStatus());

            client.commitBulkImportSession(bulkName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }

    public static void saveQueryExample()
    {
        TDClient client = TDClient.newClient();

        // Register a new scheduled query
        TDSaveQueryRequest query =
                TDSavedQuery.newBuilder(
                        "my_saved_query",
                        TDJob.Type.PRESTO,
                        "testdb",
                        "select 1",
                        "Asia/Tokyo")
                        .setCron("40 * * * *")
                        .setResult("mysql://testuser:pass@somemysql.address/somedb/sometable")
                        .build();

        client.saveQuery(query);

        // List saved queries
        List<TDSavedQuery> savedQueries = client.listSavedQueries();

        // Run a saved query
        Date scheduledTime = new Date(System.currentTimeMillis());
        client.startSavedQuery(query.getName(), scheduledTime);

        // Get saved query job history (first page)
        TDSavedQueryHistory firstPage = client.getSavedQueryHistory(query.getName());

        // Get second page
        long from = firstPage.getTo().get();
        long to = from + 20;
        TDSavedQueryHistory secondPage = client.getSavedQueryHistory(query.getName(), from, to);

        // Get result of last job
        TDJob lastJob = firstPage.getHistory().get(0);
        System.out.println("Last job:" + lastJob);

        // Update a saved query
        TDSavedQueryUpdateRequest updateRequest =
                TDSavedQuery.newUpdateRequestBuilder()
                        .setQuery("select 2")
                        .setDelay(3600)
                        .build();
        client.updateSavedQuery("my_saved_query", updateRequest);

        // Delete a saved query
        client.deleteSavedQuery(query.getName());
    }
}
