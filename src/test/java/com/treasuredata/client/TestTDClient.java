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
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDTable;
import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.treasuredata.client.TDClientConfig.firstNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestTDClient
{
    private static final Logger logger = LoggerFactory.getLogger(TestTDClient.class);
    private TDClient client;

    @Before
    public void setUp()
            throws Exception
    {
        client = new TDClient();
    }

    @After
    public void tearDown()
            throws Exception
    {
        client.close();
    }

    @Test
    public void readMavenVersion()
            throws MalformedURLException
    {
        String v = TDClient.readMavenVersion(TestTDClient.class.getResource("/pom.properties"));
        assertEquals("0.6.x", v);

        String v2 = TDClient.readMavenVersion(new URL("http://localhost/"));
        assertEquals("unknown", v2);
    }

    @Test
    public void dbNameValidation()
    {
        TDClient.validateDatabaseName("abc01234_134");
        TDClient.validateTableName("ab430_9");
        try {
            TDClient.validateDatabaseName("a");
        }
        catch (TDClientException e) {
            assertEquals(TDClientException.ErrorType.INVALID_INPUT, e.getErrorType());
        }
        try {
            TDClient.validateDatabaseName("a---4");
        }
        catch (TDClientException e) {
            assertEquals(TDClientException.ErrorType.INVALID_INPUT, e.getErrorType());
        }
    }

    @Test
    public void serverStatus()
            throws JSONException
    {
        String status = client.serverStatus();
        logger.info(status);
        JSONObject s = new JSONObject(status);
        assertEquals("ok", s.getString("status"));
    }

    @Test
    public void listDatabases()
            throws Exception
    {
        List<String> dbList = client.listDatabases();
        assertTrue("should contain sample_datasets", dbList.contains("sample_datasets"));

        logger.debug(Joiner.on(", ").join(dbList));
    }

    @Test
    public void listTables()
            throws Exception
    {
        List<TDTable> tableList = client.listTables("sample_datasets");
        assertTrue(tableList.size() >= 2);
        logger.debug(Joiner.on(", ").join(tableList));

        for (final TDTable t : tableList) {
            if (t.getName().equals("nasdaq")) {
                assertTrue(t.getColumns().size() == 6);
            }
            else if (t.getName().equals("www_access")) {
                assertTrue(t.getColumns().size() == 8);
            }
        }
    }

    @Test
    public void listJobs()
            throws Exception
    {
        TDJobList jobs = client.listJobs();
        logger.debug("job list: " + jobs);

        TDJobList jobsInAnIDRange = client.listJobs(34022478, 34022600);
        logger.debug("job list: " + jobsInAnIDRange);
        assertTrue(jobsInAnIDRange.getJobs().size() > 0);
    }

    private TDJobSummary waitJobCompletion(String jobId)
            throws InterruptedException
    {
        int retryCount = 0;
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);

        TDJobSummary tdJob = null;
        do {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException(String.format("waiting job %s has timed out", jobId));
            }
            Thread.sleep(1000);
            tdJob = client.jobStatus(jobId);
            logger.debug("job status: " + tdJob);
            retryCount++;
        }
        while (retryCount < 10 && !tdJob.getStatus().isFinished());
        return tdJob;
    }

    @Test
    public void submitJob()
            throws Exception
    {
        String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java test\nselect count(*) from nasdaq"));
        logger.debug("job id: " + jobId);

        int retryCount = 0;

        TDJobSummary tdJob = waitJobCompletion(jobId);
        TDJob jobInfo = client.jobInfo(jobId);
        logger.debug("job show result: " + tdJob);

        JSONArray array = client.jobResult(jobId, TDResultFormat.JSON, new Function<InputStream, JSONArray>()
        {
            @Override
            public JSONArray apply(InputStream input)
            {
                try {
                    String result = new String(ByteStreams.toByteArray(input), StandardCharsets.UTF_8);
                    logger.info("result:\n" + result);
                    return new JSONArray(result);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        });
        assertEquals(1, array.length());
        assertEquals(8807278, array.getLong(0));

        // test msgpack.gz format
        client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Object>()
        {
            @Override
            public Object apply(InputStream input)
            {
                try {
                    logger.debug("Reading job result in msgpack.gz");
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                    int rowCount = 0;
                    while (unpacker.hasNext()) {
                        ArrayValue array = unpacker.unpackValue().asArrayValue();
                        assertEquals(1, array.size());
                        int numColumns = array.get(0).asIntegerValue().toInt();
                        assertEquals(8807278, numColumns);
                        rowCount++;
                    }
                    assertEquals(rowCount, 1);
                    return null;
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }

    @Test
    public void submitJobWithResultOutput()
            throws Exception
    {
        client.deleteTableIfExists(SAMPLE_DB, "sample_output");
        String resultOutput = String.format("td://%s@/%s/sample_output?mode=replace", TDClientConfig.currentConfig().getApiKey().get(), SAMPLE_DB);
        String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java test\nselect count(*) from nasdaq", resultOutput));
        TDJobSummary tdJob = waitJobCompletion(jobId);
        client.existsTable(SAMPLE_DB, "sample_output");
    }

    @Test
    public void killJob()
    {
        String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java job kill test\n select time from nasdaq"));
        client.killJob(jobId);
        TDJobSummary summary = client.jobStatus(jobId);
        assertEquals(TDJob.Status.KILLED, summary.getStatus());
    }

    @Test
    public void invalidJobStatus()
    {
        try {
            TDJobSummary invalidJob = client.jobStatus("xxxxxx");
            logger.debug("invalid job: " + invalidJob);

            fail("should not reach here");
        }
        catch (TDClientException e) {
            assertEquals(TDClientException.ErrorType.TARGET_NOT_FOUND, e.getErrorType());
        }
    }

    private static final String SAMPLE_DB = "_tdclient_test";
    private static final String SAMPLE_TABLE = "sample";
    private static final String BULK_IMPORT_TABLE = "sample_bi";

    @Test
    public void databaseOperation()
    {
        if (System.getenv("CIRCLE_SHA1") != null) {
            // Skip modifying DB at CircleCI since the test user has no authority to modify databases
            logger.info("Skip create/delete database test at CircleCI");
        }
        else {
            client.deleteDatabaseIfExists(SAMPLE_DB + "_1");
            client.createDatabaseIfNotExists(SAMPLE_DB + "_1");
        }
    }

    @Test
    public void tableOperation()
            throws Exception
    {
        client.deleteTableIfExists(SAMPLE_DB, SAMPLE_TABLE);

        client.createTable(SAMPLE_DB, SAMPLE_TABLE);
        client.deleteTable(SAMPLE_DB, SAMPLE_TABLE);

        client.createTableIfNotExists(SAMPLE_DB, SAMPLE_TABLE);

        assertFalse(client.existsTable(SAMPLE_DB + "_nonexistent", "sample"));

        // conflict test
        try {
            client.createTable(SAMPLE_DB, SAMPLE_TABLE);
            fail("should not reach here");
        }
        catch (TDClientHttpConflictException e) {
            // OK
            assertEquals(HttpStatus.CONFLICT_409, e.getStatusCode());
        }

        // not found test
        try {
            client.listTables("__unknown__database");
            fail("should not reach here");
        }
        catch (TDClientHttpNotFoundException e) {
            // OK
            assertEquals(HttpStatus.NOT_FOUND_404, e.getStatusCode());
        }

        // rename
        String newTableName = SAMPLE_TABLE + "_renamed";
        client.deleteTableIfExists(SAMPLE_DB, newTableName);
        client.renameTable(SAMPLE_DB, SAMPLE_TABLE, newTableName);
        assertTrue(client.existsTable(SAMPLE_DB, newTableName));
        assertFalse(client.existsTable(SAMPLE_DB, SAMPLE_TABLE));
    }

    private String queryResult(String database, String sql)
            throws InterruptedException
    {
        String jobId = client.submit(TDJobRequest.newPrestoQuery(database, sql));
        waitJobCompletion(jobId);
        return client.jobResult(jobId, TDResultFormat.CSV, new Function<InputStream, String>()
        {
            @Override
            public String apply(InputStream input)
            {
                try {
                    String result = CharStreams.toString(new InputStreamReader(input));
                    logger.info(result);
                    return result;
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }

    @Test
    public void swapTest()
            throws Exception
    {
        // swap
        String t1 = SAMPLE_TABLE + "_1";
        String t2 = SAMPLE_TABLE + "_2";
        client.deleteTableIfExists(SAMPLE_DB, t1);
        client.deleteTableIfExists(SAMPLE_DB, t2);
        client.createTableIfNotExists(SAMPLE_DB, t1);
        client.createTableIfNotExists(SAMPLE_DB, t2);

        String job1 = client.submit(TDJobRequest.newPrestoQuery(SAMPLE_DB, String.format("INSERT INTO %s select 1", t1)));
        String job2 = client.submit(TDJobRequest.newPrestoQuery(SAMPLE_DB, String.format("INSERT INTO %s select 2", t2)));
        waitJobCompletion(job1);
        waitJobCompletion(job2);

        String before1 = queryResult(SAMPLE_DB, String.format("select * from %s", t1));
        String before2 = queryResult(SAMPLE_DB, String.format("select * from %s", t2));

        client.swapTables(SAMPLE_DB, t1, t2);

        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        String after1 = queryResult(SAMPLE_DB, String.format("select * from %s", t1));
        String after2 = queryResult(SAMPLE_DB, String.format("select * from %s", t2));

        assertEquals(before1, after2);
        assertEquals(before2, after1);
    }

    @Test
    public void testBulkImport()
            throws Exception
    {
        client.deleteTableIfExists(SAMPLE_DB, BULK_IMPORT_TABLE);
        client.createTableIfNotExists(SAMPLE_DB, BULK_IMPORT_TABLE);

        final int numRowsInPart = 10;
        final int numParts = 3;
        String dateStr = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        final String session = "td-client-java-test-session-" + dateStr;
        try {
            client.createBulkImportSession(session, SAMPLE_DB, BULK_IMPORT_TABLE);

            List<TDBulkImportSession> sessionList = client.listBulkImportSessions();
            TDBulkImportSession foundInList = Iterables.find(sessionList, new Predicate<TDBulkImportSession>()
            {
                @Override
                public boolean apply(TDBulkImportSession input)
                {
                    return input.getName().equals(session);
                }
            });

            TDBulkImportSession bs = client.getBulkImportSession(session);
            logger.info("bulk import session: {}, error message: {}", bs.getJobId(), bs.getErrorMessage());
            assertEquals(session, bs.getName());
            assertEquals(SAMPLE_DB, bs.getDatabaseName());
            assertEquals(BULK_IMPORT_TABLE, bs.getTableName());
            assertTrue(bs.isUploading());

            assertEquals(foundInList.getStatus(), bs.getStatus());

            int count = 0;
            final long time = System.currentTimeMillis() / 1000;

            // Upload part 0, 1, 2
            for (int i = 0; i < 3; ++i) {
                String partName = "bip" + i;
                // Prepare msgpack.gz
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                OutputStream out = new GZIPOutputStream(buf);
                MessagePacker packer = MessagePack.newDefaultPacker(out);
                for (int n = 0; n < numRowsInPart; ++n) {
                    ValueFactory.MapBuilder b = ValueFactory.newMapBuilder();
                    b.put(ValueFactory.newString("time"), ValueFactory.newInteger(time + count));
                    b.put(ValueFactory.newString("event"), ValueFactory.newString("log" + count));
                    b.put(ValueFactory.newString("description"), ValueFactory.newString("sample data"));
                    packer.packValue(b.build());
                    count += 1;
                }
                // Embed an error record
                packer.packValue(ValueFactory.newMap(new Value[] {ValueFactory.newNil(), ValueFactory.newString("invalid data")}));

                packer.close();
                out.close();

                File tmpFile = File.createTempFile(partName, ".msgpack.gz", new File("target"));
                Files.write(tmpFile.toPath(), buf.toByteArray());
                client.uploadBulkImportPart(session, partName, tmpFile);

                // list parts
                List<String> parts = client.listBulkImportParts(session);
                assertTrue(parts.contains(partName));

                // freeze test
                client.freezeBulkImportSession(session);

                // unfreeze test
                client.unfreezeBulkImportSession(session);
            }

            // delete the last
            client.deleteBulkImportPart(session, "bip2");

            List<String> parts = client.listBulkImportParts(session);
            assertTrue(!parts.contains("bip2"));

            // Freeze the session
            client.freezeBulkImportSession(session);

            // Perform the session
            client.performBulkImportSession(session);

            // Wait the perform completion
            long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
            bs = client.getBulkImportSession(session);
            while (bs.getStatus() == TDBulkImportSession.ImportStatus.PERFORMING) {
                assertFalse(bs.isUploading());
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("timeout error: bulk import perform");
                }
                logger.info("Waiting bulk import completion");
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                bs = client.getBulkImportSession(session);
            }

            // Check session contents
            assertTrue(bs.hasErrorOnPerform());
            logger.debug(bs.getErrorMessage());

            // Error record check
            int errorCount = client.getBulkImportErrorRecords(session, new Function<InputStream, Integer>()
            {
                int errorRecordCount = 0;

                @Override
                public Integer apply(InputStream input)
                {
                    try {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                        while (unpacker.hasNext()) {
                            Value v = unpacker.unpackValue();
                            logger.info("error record: " + v);
                            errorRecordCount += 1;
                        }
                        return errorRecordCount;
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });

            final int numValidParts = numParts - 1;
            assertEquals(numValidParts, errorCount);
            assertEquals(0, bs.getErrorParts());
            assertEquals(numValidParts, bs.getValidParts());
            assertEquals(numValidParts, bs.getErrorRecords());
            assertEquals(numValidParts * numRowsInPart, bs.getValidRecords());

            // Commit the session
            deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
            client.commitBulkImportSession(session);

            // Wait the commit completion
            bs = client.getBulkImportSession(session);
            while (bs.getStatus() != TDBulkImportSession.ImportStatus.COMMITTED) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("timeout error: bulk import commit");
                }
                logger.info("Waiting bulk import perform step completion");
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                bs = client.getBulkImportSession(session);
            }

            // Check the data
            TDTable imported = Iterables.find(client.listTables(SAMPLE_DB), new Predicate<TDTable>()
            {
                @Override
                public boolean apply(TDTable input)
                {
                    return input.getName().equals(BULK_IMPORT_TABLE);
                }
            });

            assertEquals(numRowsInPart * 2, imported.getRowCount());
            List<TDColumn> columns = imported.getColumns();
            logger.info(Joiner.on(", ").join(columns));
            assertEquals(2, columns.size()); // event, description, (time)
        }
        finally {
            client.deleteBulkImportSession(session);
        }
    }

    @Test
    public void authenticate()
            throws Exception
    {
        // authenticate() method should retrieve apikey, and set it to the TDClient
        Properties p = TDClientConfig.readTDConf();
        TDClient client = new TDClient(new TDClientConfig.Builder().build()); // Set no API key
        String user = firstNonNull(p.getProperty("user"), System.getenv("TD_USER"));
        String password = firstNonNull(p.getProperty("password"), System.getenv("TD_PASS"));
        TDClient newClient = client.authenticate(user, password);
        List<TDTable> tableList = newClient.listTables("sample_datasets");
        assertTrue(tableList.size() >= 2);
    }

    @Test
    public void wrongApiKey()
    {
        TDClient client = new TDClient(new TDClientConfig.Builder().setApiKey("1/xxfasdfafd").build()); // Set a wrong API key
        try {
            client.listDatabases();
            fail("should not reach here");
        }
        catch (TDClientHttpUnauthorizedException e) {
            // OK
            assertEquals(HttpStatus.UNAUTHORIZED_401, e.getStatusCode());
        }
    }
}
