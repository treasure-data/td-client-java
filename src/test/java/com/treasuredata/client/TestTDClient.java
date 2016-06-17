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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.treasuredata.client.model.ObjectMappers;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDBulkLoadSessionStartResult;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDColumnType;
import com.treasuredata.client.model.TDDatabase;
import com.treasuredata.client.model.TDExportFileFormatType;
import com.treasuredata.client.model.TDExportJobRequest;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDPartialDeleteJob;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import com.treasuredata.client.model.TDSavedQueryHistory;
import com.treasuredata.client.model.TDSavedQueryUpdateRequest;
import com.treasuredata.client.model.TDTable;
import com.treasuredata.client.model.TDUser;
import com.treasuredata.client.model.TDUserList;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestTDClient
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final Logger logger = LoggerFactory.getLogger(TestTDClient.class);

    private static final String SAMPLE_DB = "_tdclient_test";

    private TDClient client;

    private MockWebServer server;

    @Before
    public void setUp()
            throws Exception
    {
        client = TDClient.newClient();
        server = new MockWebServer();
    }

    @After
    public void tearDown()
            throws Exception
    {
        client.close();
        server.shutdown();
    }

    @Test
    public void readMavenVersion()
            throws MalformedURLException
    {
        String v = TDClient.readMavenVersion(TestTDClient.class.getResource("/pom.properties"));
        assertEquals("0.6.x", v);

        logger.warn("Running failing test for readMavenVersion");
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
        List<String> dbList = client.listDatabaseNames();
        assertTrue("should contain sample_datasets", dbList.contains("sample_datasets"));

        String dbListStr = Joiner.on(", ").join(dbList);
        logger.debug(dbListStr);

        List<TDDatabase> detailedDBList = client.listDatabases();
        Iterable<String> dbStr = Iterables.transform(detailedDBList, new Function<TDDatabase, String>()
        {
            @Override
            public String apply(TDDatabase input)
            {
                String summary = String.format("name:%s, count:%s, createdAt:%s, updatedAt:%s, organization:%s, permission:%s", input.getName(), input.getCount(), input.getCreatedAt(), input.getUpdatedAt(), input.getOrganization(), input.getPermission());
                return summary;
            }
        });

        String detailedDbListStr = Joiner.on(", ").join(dbStr);
        logger.trace(detailedDbListStr);
    }

    @Test
    public void listTables()
            throws Exception
    {
        List<TDTable> tableList = client.listTables("sample_datasets");
        assertTrue(tableList.size() >= 2);
        logger.debug(Joiner.on(", ").join(tableList));

        Set<TDTable> tableSet = new HashSet<>();
        for (final TDTable t : tableList) {
            logger.info("id: " + t.getId());
            logger.info("type: " + t.getType());
            logger.info("estimated size:" + t.getEstimatedStorageSize());
            logger.info("last log timestamp: " + t.getLastLogTimeStamp());
            logger.info("expire days:" + t.getExpireDays());
            logger.info("created at: " + t.getCreatedAt());
            logger.info("updated at: " + t.getUpdatedAt());
            if (t.getName().equals("nasdaq")) {
                assertTrue(t.getColumns().size() == 6);
            }
            else if (t.getName().equals("www_access")) {
                assertTrue(t.getColumns().size() == 8);
            }
            // To use equals and hashCode
            tableSet.add(t);
        }

        // equality tests
        for (TDTable t : tableSet) {
            tableSet.contains(t);
        }

        for (int i = 0; i < tableList.size(); ++i) {
            for (int j = 0; j < tableList.size(); ++j) {
                if (i == j) {
                    assertEquals(tableList.get(i), tableList.get(j));
                }
                else {
                    assertFalse(tableList.get(i).equals(tableList.get(j)));
                }
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

        // Check getters
        Iterable<Method> getters = Iterables.filter(ImmutableList.copyOf(TDJob.class.getDeclaredMethods()), new Predicate<Method>()
        {
            @Override
            public boolean apply(Method input)
            {
                return input.getName().startsWith("get");
            }
        });
        // Call getters
        for (TDJob job : jobs.getJobs()) {
            for (Method m : getters) {
                m.invoke(job);
            }
        }
    }

    private TDJobSummary waitJobCompletion(String jobId)
            throws InterruptedException
    {
        int retryCount = 0;
        ExponentialBackOff backoff = new ExponentialBackOff();
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        TDJobSummary tdJob = null;
        do {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException(String.format("waiting job %s has timed out", jobId));
            }
            int nextWait = backoff.nextWaitTimeMillis();
            logger.debug(String.format("Run job status check in %.2f sec.", nextWait / 1000.0));
            Thread.sleep(nextWait);
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
        String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java test\nselect count(*) cnt from nasdaq"));
        logger.debug("job id: " + jobId);

        int retryCount = 0;

        TDJobSummary tdJob = waitJobCompletion(jobId);
        TDJob jobInfo = client.jobInfo(jobId);
        logger.debug("job show result: " + tdJob);
        logger.debug("job info: " + jobInfo);
        Optional<String> schema = jobInfo.getResultSchema();
        assertTrue(schema.isPresent());
        assertEquals("[[\"cnt\", \"bigint\"]]", schema.get());

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
        String resultOutput = String.format("td://%s@/%s/sample_output?mode=replace", client.config.apiKey.get(), SAMPLE_DB);
        String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java test\nselect count(*) from nasdaq", resultOutput));
        TDJobSummary tdJob = waitJobCompletion(jobId);
        client.existsTable(SAMPLE_DB, "sample_output");
    }

    @Test
    public void submitJobWithPoolName()
            throws Exception
    {
        client.deleteTableIfExists(SAMPLE_DB, "sample_output");
        String poolName = "hadoop2";
        String jobId = client.submit(TDJobRequest.newHiveQuery("sample_datasets", "-- td-client-java test\nselect count(*) from nasdaq", null, poolName));
        TDJobSummary tdJob = waitJobCompletion(jobId);
        client.existsTable(SAMPLE_DB, "sample_output");
    }

    @Test
    public void submitJobWithScheduledTime()
            throws Exception
    {
        long scheduledTime = 1368080054;
        TDJobRequest request = new TDJobRequestBuilder()
                .setType(TDJob.Type.PRESTO)
                .setDatabase("sample_datasets")
                .setQuery("select TD_SCHEDULED_TIME()")
                .setScheduledTime(scheduledTime)
                .createTDJobRequest();
        String jobId = client.submit(request);
        waitJobCompletion(jobId);

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
        assertEquals(scheduledTime, array.getLong(0));
    }

    @Test
    public void submitJobWithDomainKey()
            throws Exception
    {
        String domainKey = randomDomainKey();

        TDJobRequest request1 = new TDJobRequestBuilder()
                .setType(TDJob.Type.PRESTO)
                .setDatabase("sample_datasets")
                .setQuery("select 1")
                .setDomainKey(domainKey)
                .createTDJobRequest();
        String jobId = client.submit(request1);
        waitJobCompletion(jobId);

        TDJobRequest request2 = new TDJobRequestBuilder()
                .setType(TDJob.Type.PRESTO)
                .setDatabase("sample_datasets")
                .setQuery("select 1")
                .setDomainKey(domainKey)
                .createTDJobRequest();

        try {
            client.submit(request2);
            fail("Expected " + TDClientHttpConflictException.class.getName());
        }
        catch (TDClientHttpConflictException e) {
            assertThat(e.getConflictsWith(), is(Optional.of(jobId)));
        }
    }

    @Test
    public void submitBulkLoadJob()
            throws Exception
    {
        ObjectNode config = JsonNodeFactory.instance.objectNode();
        ObjectNode in = JsonNodeFactory.instance.objectNode();
        in.put("type", "s3");
        config.put("in", in);
        client.createDatabaseIfNotExists(SAMPLE_DB);
        client.createTableIfNotExists(SAMPLE_DB, "sample_output");
        String jobId = client.submit(TDJobRequest.newBulkLoad(SAMPLE_DB, "sample_output", config));
        TDJobSummary tdJob = waitJobCompletion(jobId);
        // this job will fail because of lack of parameters for s3 input plugin
    }

    @Test
    public void startSavedQuery()
            throws Exception
    {
        Date scheduledTime = new Date(1457046000 * 1000L);
        try {
            String jobId = client.startSavedQuery("no method to save a schedule yet", scheduledTime);
            TDJobSummary tdJob = waitJobCompletion(jobId);
            fail();
        }
        catch (TDClientHttpNotFoundException e) {
            // OK
        }
    }

    @Test
    public void submitExportJob()
            throws Exception
    {
        TDExportJobRequest jobRequest = new TDExportJobRequest(
                SAMPLE_DB,
                "sample_output",
                new Date(0L),
                new Date(1456522300L * 1000),
                TDExportFileFormatType.JSONL_GZ,
                "access key id",
                "secret access key",
                "bucket",
                "prefix/",
                Optional.<String>absent());
        client.createDatabaseIfNotExists(SAMPLE_DB);
        client.createTableIfNotExists(SAMPLE_DB, "sample_output");
        String jobId = client.submitExportJob(jobRequest);
        TDJobSummary tdJob = waitJobCompletion(jobId);
        // this job will do nothing because sample_output table is empty
    }

    @Test
    public void killJob()
            throws Exception
    {
        String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java job kill test\n select time from nasdaq"));
        client.killJob(jobId);
        waitJobCompletion(jobId);
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

    @Test
    public void databaseOperation()
    {
        if (System.getenv("CIRCLE_SHA1") != null) {
            // Skip modifying DB at CircleCI since the test user has no authority to modify databases
            logger.info("Skip create/delete database test at CircleCI");
        }
        else {
            String dbName = newTemporaryName(SAMPLE_DB);
            try {
                client.deleteDatabaseIfExists(dbName);
                client.createDatabaseIfNotExists(dbName);
            }
            finally {
                client.deleteDatabaseIfExists(dbName);
            }
        }
    }

    private Optional<TDTable> findTable(String databaseName, String tableName)
    {
        for (TDTable table : client.listTables(databaseName)) {
            if (table.getName().equals(tableName)) {
                return Optional.of(table);
            }
        }
        return Optional.absent();
    }

    @Test
    public void tableOperation()
            throws Exception
    {
        String t = newTemporaryName("sample");
        String newTableName = t + "_renamed";

        try {
            client.deleteTableIfExists(SAMPLE_DB, t);

            client.createTable(SAMPLE_DB, t);
            client.deleteTable(SAMPLE_DB, t);

            client.createTableIfNotExists(SAMPLE_DB, t);

            assertFalse(client.existsTable(SAMPLE_DB + "_nonexistent", t));

            // conflict test
            try {
                client.createTable(SAMPLE_DB, t);
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

            byte[] keyName = "int_col_key_name".getBytes(StandardCharsets.UTF_8);
            // schema test
            TDTable targetTable = findTable(SAMPLE_DB, t).get();
            List<TDColumn> newSchema = ImmutableList.<TDColumn>builder()
                    .addAll(targetTable.getSchema())
                    .add(new TDColumn("int_col", TDColumnType.INT, keyName))
                    .build();
            client.updateTableSchema(SAMPLE_DB, t, newSchema);
            TDTable updatedTable = findTable(SAMPLE_DB, t).get();
            logger.debug(updatedTable.toString());
            assertTrue("should have updated column", updatedTable.getSchema().contains(new TDColumn("int_col", TDColumnType.INT, keyName)));

            // rename
            client.deleteTableIfExists(SAMPLE_DB, newTableName);
            client.renameTable(SAMPLE_DB, t, newTableName);
            assertTrue(client.existsTable(SAMPLE_DB, newTableName));
            assertFalse(client.existsTable(SAMPLE_DB, t));
        }
        finally {
            client.deleteTableIfExists(SAMPLE_DB, t);
            client.deleteTableIfExists(SAMPLE_DB, newTableName);
        }
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

    private static String newTemporaryName(String prefix)
    {
        String dateStr = new SimpleDateFormat("yyyyMMddhhmmssSSS").format(new Date());
        return prefix + "_" + dateStr;
    }

    @Test
    public void partialDeleteTest()
            throws Exception
    {
        String t = newTemporaryName("td_client_test");
        try {
            client.deleteTableIfExists(SAMPLE_DB, t);

            String jobId = client.submit(TDJobRequest.newPrestoQuery(SAMPLE_DB,
                    String.format("CREATE TABLE %s AS SELECT * FROM (VALUES TD_TIME_PARSE('2015-01-01', 'UTC'), TD_TIME_PARSE('2015-02-01', 'UTC')) as sample(time)", t, t)));

            waitJobCompletion(jobId);

            String before = queryResult(SAMPLE_DB, String.format("SELECT * FROM %s", t));

            assertTrue(before.contains("1420070400"));
            assertTrue(before.contains("1422748800"));

            // delete 2015-01-01 entry
            try {
                client.partialDelete(SAMPLE_DB, t, 1420070400, 1420070400 + 1);
                fail("should not reach here");
            }
            catch (TDClientException e) {
                assertEquals(TDClientException.ErrorType.INVALID_INPUT, e.getErrorType());
            }
            long from = 1420070400 - (1420070400 % 3600);
            long to = from + 3600;
            TDPartialDeleteJob partialDeleteJob = client.partialDelete(SAMPLE_DB, t, from, to);
            logger.debug("partial delete job: " + partialDeleteJob);
            assertEquals(from, partialDeleteJob.getFrom());
            assertEquals(to, partialDeleteJob.getTo());
            assertEquals(SAMPLE_DB, partialDeleteJob.getDatabase());
            assertEquals(t, partialDeleteJob.getTable());

            waitJobCompletion(partialDeleteJob.getJobId());

            String after = queryResult(SAMPLE_DB, String.format("SELECT * FROM %s", t));
            assertFalse(after.contains("1420070400"));
            assertTrue(after.contains("1422748800"));
        }
        finally {
            client.deleteTableIfExists(SAMPLE_DB, t);
        }
    }

    @Test
    public void swapTest()
            throws Exception
    {
        // swap
        String t1 = newTemporaryName("sample1");
        String t2 = newTemporaryName("sample2");
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
        final String bulkImportTable = newTemporaryName("sample_bi");
        client.deleteTableIfExists(SAMPLE_DB, bulkImportTable);
        client.createTableIfNotExists(SAMPLE_DB, bulkImportTable);

        final int numRowsInPart = 10;
        final int numParts = 3;
        String dateStr = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        final String session = "td-client-java-test-session-" + dateStr;
        try {
            client.createBulkImportSession(session, SAMPLE_DB, bulkImportTable);

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
            assertEquals(bulkImportTable, bs.getTableName());
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
            ExponentialBackOff backoff = new ExponentialBackOff();
            long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
            bs = client.getBulkImportSession(session);
            while (bs.getStatus() == TDBulkImportSession.ImportStatus.PERFORMING) {
                assertFalse(bs.isUploading());
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("timeout error: bulk import perform");
                }
                logger.debug("Waiting bulk import completion");
                Thread.sleep(backoff.nextWaitTimeMillis());
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
                    return input.getName().equals(bulkImportTable);
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

    public static String firstNonNull(String... values)
    {
        for (String v : values) {
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    @Test
    public void authenticate()
            throws Exception
    {
        // authenticate() method should retrieve apikey, and set it to the TDClient
        Properties p = TDClientConfig.readTDConf();
        TDClient client = new TDClientBuilder(false).build(); // Set no API key
        String user = firstNonNull(p.getProperty("user"), System.getenv("TD_USER"));
        String password = firstNonNull(p.getProperty("password"), System.getenv("TD_PASS"));
        TDClient newClient = client.authenticate(user, password);
        List<TDTable> tableList = newClient.listTables("sample_datasets");
        assertTrue(tableList.size() >= 2);
    }

    @Test
    public void wrongApiKey()
    {
        TDClient client = TDClient.newBuilder().setApiKey("1/xxfasdfafd").build(); // Set a wrong API key
        try {
            client.listDatabaseNames();
            fail("should not reach here");
        }
        catch (TDClientHttpUnauthorizedException e) {
            // OK
            assertEquals(HttpStatus.UNAUTHORIZED_401, e.getStatusCode());
        }
    }

    @Test
    public void getUserMocked()
            throws Exception
    {
        client = mockClient();

        String expectedUserJson = "{\"id\":300,\"first_name\":\"Freda\",\"last_name\":\"Schuster\",\"email\":\"1elvie.hackett@example.com\",\"phone\":\"(650) 469-3644\",\"gravatar_url\":\"https://secure.gravatar.com/avatar/0e36aa63098c5a05b4dde8ac867eb116?size=80\",\"administrator\":true,\"created_at\":\"2016-06-09T01:34:59Z\",\"updated_at\":\"2016-06-09T01:34:59Z\",\"name\":\"Freda Schuster\",\"account_owner\":true}";

        TDUser expectedUser = ObjectMappers.compactMapper().readValue(expectedUserJson, TDUser.class);

        server.enqueue(new MockResponse().setBody(expectedUserJson));

        TDUser user = client.getUser();

        assertThat(user, is(expectedUser));

        RecordedRequest recordedRequest = server.takeRequest();
        assertThat(recordedRequest.getPath(), is("/v3/user/show"));
    }

    @Test
    public void getUser()
            throws Exception
    {
        TDUser user = client.getUser();
        logger.info("user: {}", user);
    }

    @Test
    public void listUsers()
            throws Exception
    {
        TDUserList userList = client.listUsers();
        for (TDUser user : userList.getUsers()) {
            logger.info("user: {}", user);
        }
        logger.info("{} user(s)", userList.getUsers().size());
    }

    @Test
    public void listSavedQuery()
    {
        List<TDSavedQuery> savedQueries = client.listSavedQueries();
        assertTrue(savedQueries.size() > 0);
        logger.info(Joiner.on(", ").join(savedQueries));
    }

    @Test
    public void getSavedQueryHistory()
    {
        List<TDSavedQuery> allQueries = client.listSavedQueries();
        List<TDSavedQuery> queries = FluentIterable.from(allQueries)
                .limit(10)
                .toList();

        for (TDSavedQuery query : queries) {
            TDSavedQueryHistory firstPage = client.getSavedQueryHistory(query.getName());
            logger.info("count: {}, from: {}, to: {}, jobs: {}", firstPage.getCount(), firstPage.getFrom(), firstPage.getTo(), firstPage.getHistory().size());
            TDSavedQueryHistory secondPage = client.getSavedQueryHistory(query.getName(), firstPage.getTo().get(), firstPage.getTo().get() + 20L);
            logger.info("count: {}, from: {}, to: {}, jobs: {}", secondPage.getCount(), secondPage.getFrom(), secondPage.getTo(), secondPage.getHistory().size());
        }
    }

    private Optional<TDSavedQuery> findSavedQuery(String name)
    {
        List<TDSavedQuery> savedQueries = client.listSavedQueries();
        for (TDSavedQuery q : savedQueries) {
            if (q.getName().equals(name)) {
                return Optional.of(q);
            }
        }
        return Optional.absent();
    }

    private void validateSavedQuery(TDSaveQueryRequest expected, TDSavedQuery target)
    {
        assertEquals(expected.getName(), target.getName());
        assertEquals(expected.getCron(), target.getCron());
        assertEquals(expected.getType(), target.getType());
        assertEquals(expected.getQuery(), target.getQuery());
        assertEquals(expected.getTimezone(), target.getTimezone());
        assertEquals(expected.getDelay(), target.getDelay());
        assertEquals(expected.getDatabase(), target.getDatabase());
        assertEquals(expected.getPriority(), target.getPriority());
        assertEquals(expected.getRetryLimit(), target.getRetryLimit());
    }

    @Test
    public void saveAndDeleteQuery()
    {
        String queryName = newTemporaryName("td_client_test");

        TDSaveQueryRequest query = TDSavedQuery.newBuilder(
                queryName,
                TDJob.Type.PRESTO,
                SAMPLE_DB,
                "select 1",
                "Asia/Tokyo")
                .setCron("0 * * * *")
                .setPriority(-1)
                .setRetryLimit(2)
                .setResult("mysql://testuser:pass@somemysql.address/somedb/sometable")
                .build();

        try {
            TDSavedQuery result = client.saveQuery(query);
            Optional<TDSavedQuery> q = findSavedQuery(queryName);
            assertTrue(String.format("saved query %s is not found", queryName), q.isPresent());

            validateSavedQuery(query, result);
            assertTrue(result.getResult().startsWith("mysql://testuser:")); // password will be hidden
            assertTrue(result.getResult().contains("@somemysql.address/somedb/sometable"));

            // Update
            TDSavedQueryUpdateRequest query2 =
                    TDSavedQuery.newUpdateRequestBuilder()
                            .setCron("15 * * * *")
                            .setType(TDJob.Type.HIVE)
                            .setQuery("select 2")
                            .setTimezone("UTC")
                            .setDelay(20)
                            .setDatabase(SAMPLE_DB)
                            .setPriority(-1)
                            .setRetryLimit(2)
                            .setResult("mysql://testuser2:pass@somemysql.address/somedb2/sometable2")
                            .build();

            TDSaveQueryRequest expected = query2.merge(result);
            TDSavedQuery updated = client.updateSavedQuery(queryName, query2);
            validateSavedQuery(expected, updated);
            assertTrue(updated.getResult().startsWith("mysql://testuser2:")); // password will be hidden
            assertTrue(updated.getResult().contains("@somemysql.address/somedb2/sometable2"));
        }
        catch (TDClientException e) {
            logger.error("failed", e);
        }
        finally {
            client.deleteSavedQuery(queryName);
        }

        Optional<TDSavedQuery> q = findSavedQuery(queryName);
        assertTrue(String.format("saved query %s should be deleted", queryName), !q.isPresent());
    }

    @Test
    public void startBulkLoadSessionJob()
            throws Exception
    {
        client = mockClient();

        String sessionName = "foobar";
        String expectedJobId = "4711";
        String expectedPath = "/v3/bulk_loads/" + sessionName + "/jobs";
        String expectedPayload = "{}";

        server.enqueue(new MockResponse().setBody("{\"job_id\":\"4711\"}"));

        TDBulkLoadSessionStartResult result = client.startBulkLoadSession(sessionName);
        assertThat(result.getJobId(), is(expectedJobId));

        RecordedRequest recordedRequest = server.takeRequest();
        assertThat(recordedRequest.getBody().readUtf8(), is(expectedPayload));
        assertThat(recordedRequest.getPath(), is(expectedPath));
    }

    @Test
    public void startBulkLoadSessionJobWithScheduledTime()
            throws Exception
    {
        client = mockClient();

        String sessionName = "foobar";
        String expectedJobId = "4711";
        String expectedPath = "/v3/bulk_loads/" + sessionName + "/jobs";
        long scheduledTime = 123456789L;
        String expectedPayload = "{\"scheduled_time\":\"" + scheduledTime + "\"}";

        server.enqueue(new MockResponse().setBody("{\"job_id\":\"4711\"}"));

        TDBulkLoadSessionStartResult result = client.startBulkLoadSession(sessionName, scheduledTime);
        assertThat(result.getJobId(), is(expectedJobId));

        RecordedRequest recordedRequest = server.takeRequest();
        String body = recordedRequest.getBody().readUtf8();
        assertThat(body, is(expectedPayload));
        assertThat(recordedRequest.getPath(), is(expectedPath));
    }

    private TDClient mockClient()
    {
        return TDClient.newBuilder(false)
                .setUseSSL(false)
                .setEndpoint(server.getHostName())
                .setPort(server.getPort())
                .build();
    }

    private String randomDomainKey()
    {
        return "td-client-java-test-" + UUID.randomUUID();
    }
}
