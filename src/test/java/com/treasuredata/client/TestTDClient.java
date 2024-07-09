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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.treasuredata.client.model.ObjectMappers;
import com.treasuredata.client.model.TDBulkImportSession;
import com.treasuredata.client.model.TDBulkLoadSessionStartRequest;
import com.treasuredata.client.model.TDBulkLoadSessionStartResult;
import com.treasuredata.client.model.TDColumn;
import com.treasuredata.client.model.TDColumnType;
import com.treasuredata.client.model.TDDatabase;
import com.treasuredata.client.model.TDExportFileFormatType;
import com.treasuredata.client.model.TDExportJobRequest;
import com.treasuredata.client.model.TDExportResultJobRequest;
import com.treasuredata.client.model.TDFederatedQueryConfig;
import com.treasuredata.client.model.TDImportResult;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJob.EngineVersion;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDPartialDeleteJob;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDSaveQueryRequest;
import com.treasuredata.client.model.TDSavedQuery;
import com.treasuredata.client.model.TDSavedQueryHistory;
import com.treasuredata.client.model.TDSavedQueryStartRequest;
import com.treasuredata.client.model.TDSavedQueryUpdateRequest;
import com.treasuredata.client.model.TDTable;
import com.treasuredata.client.model.TDTableDistribution;
import com.treasuredata.client.model.TDUser;
import com.treasuredata.client.model.TDUserList;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.treasuredata.client.TDClientConfig.ENV_TD_CLIENT_APIKEY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 */
public class TestTDClient
{
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(TestTDClient.class);

    private static final String SAMPLE_DB;

    static {
        SAMPLE_DB = "_tdclient_test" + System.getProperty("java.version").replaceAll("\\.", "_");
    }

    private TDClient client;

    private MockWebServer server;

    private List<String> savedQueries = new ArrayList<>();

    @BeforeEach
    public void setUp()
            throws Exception
    {
        client = TDClient.newClient();
        server = new MockWebServer();
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        try (TDClient client1 = client;
                MockWebServer unused = server) {
            for (String name : savedQueries) {
                try {
                    client1.deleteSavedQuery(name);
                }
                catch (Exception e) {
                    logger.error("Failed to delete query: {}", name, e);
                }
            }
        }
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
            throws JsonProcessingException
    {
        String status = client.serverStatus();
        logger.info(status);
        Map<String, String> s = objectMapper.readValue(status, Map.class);
        assertEquals("ok", s.get("status"));
    }

    @Test
    public void showDatabase()
            throws Exception
    {
        String databaseName = "sample_datasets";
        TDDatabase dbDetail = client.showDatabase(databaseName);
        assertEquals(databaseName, dbDetail.getName(), "should match in sample_datasets");
        assertTrue(Integer.parseInt(dbDetail.getId()) > 0, "should be positive");

        logger.debug(dbDetail.toString());
    }

    @Test
    public void listDatabases()
            throws Exception
    {
        List<String> dbList = client.listDatabaseNames();
        assertTrue(dbList.contains("sample_datasets"), "should contain sample_datasets");

        String dbListStr = String.join(", ", dbList);
        logger.debug(dbListStr);

        List<TDDatabase> detailedDBList = client.listDatabases();
        Iterable<String> dbStr = detailedDBList.stream().map(input -> {
            String summary = String.format("name:%s, count:%s, createdAt:%s, updatedAt:%s, organization:%s, permission:%s", input.getName(), input.getCount(), input.getCreatedAt(), input.getUpdatedAt(), input.getOrganization(), input.getPermission());
            return summary;
        }).collect(Collectors.toList());

        String detailedDbListStr = String.join(", ", dbStr);
        logger.trace(detailedDbListStr);
    }

    @Test
    public void showTables()
            throws Exception
    {
        TDTable table;

        // nasdaq
        table = client.showTable("sample_datasets", "nasdaq");
        assertEquals(6, table.getColumns().size());

        // www_access
        table = client.showTable("sample_datasets", "www_access");
        assertEquals(8, table.getColumns().size());
    }

    @Test
    public void listTables()
            throws Exception
    {
        List<TDTable> tableList = client.listTables("sample_datasets");
        assertTrue(tableList.size() >= 2);
        logger.debug(tableList.stream().map(TDTable::toString).collect(Collectors.joining(", ")));

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
                assertEquals(6, t.getColumns().size());
            }
            else if (t.getName().equals("www_access")) {
                assertEquals(8, t.getColumns().size());
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

        TDJobList jobsInAnIDDefault = client.listJobs();
        logger.debug("job list: " + jobsInAnIDDefault);
        assertEquals(20, jobsInAnIDDefault.getJobs().size());

        TDJobList jobsInAnIDRange = client.listJobs(0, 100);
        logger.debug("job list: " + jobsInAnIDRange);
        assertEquals(101, jobsInAnIDRange.getJobs().size());

        // Check getters
        Iterable<Method> getters = Arrays.stream(TDJob.class.getDeclaredMethods()).filter(input -> {
            return input.getName().startsWith("get");
        }).collect(Collectors.toList());
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
        BackOff backoff = new ExponentialBackOff();
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

        String[] array = client.jobResult(jobId, TDResultFormat.JSON, input -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                String result = reader.lines().collect(Collectors.joining());
                logger.info("result:\n" + result);
                return objectMapper.readValue(result, String[].class);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(1, array.length);
        assertEquals(1, jobInfo.getNumRecords());
        assertTrue(Long.parseLong(array[0]) > 0);

        // test msgpack.gz format
        client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, input -> {
            try {
                logger.debug("Reading job result in msgpack.gz");
                MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                int rowCount = 0;
                while (unpacker.hasNext()) {
                    ArrayValue array1 = unpacker.unpackValue().asArrayValue();
                    assertEquals(1, array1.size());
                    int numRows = array1.get(0).asIntegerValue().toInt();
                    assertTrue(numRows > 0);
                    rowCount++;
                }
                assertEquals(rowCount, 1);
                return null;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void submitJobWithInvalidEngineVersionPresto()
            throws Exception
    {
        //Invalid engine_version must throw TDClientHttpException
        try {
            submitJobWithEngineVersion(TDJob.Type.PRESTO, Optional.of(TDJob.EngineVersion.fromString("AAAAAA")));
        }
        catch (TDClientHttpException te) {
            assertEquals(te.getStatusCode(), 422);
            assertTrue(te.getMessage().matches(".*Job engine version is invalid.*"));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.toString());
        }
    }

    @Test
    public void submitJobWithValidEngineVersionPresto()
            throws Exception
    {
        // Valid engine_version must be accepted
        try {
            submitJobWithEngineVersion(TDJob.Type.PRESTO, Optional.of(EngineVersion.fromString("stable")));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.toString());
        }
    }

    @Test
    public void submitJobWithInvalidEngineVersionHive()
            throws Exception
    {
        //Invalid engine_version must throw TDClientHttpException
        try {
            submitJobWithEngineVersion(TDJob.Type.HIVE, Optional.of(TDJob.EngineVersion.fromString("AAAAAA")));
        }
        catch (TDClientHttpException te) {
            assertEquals(te.getStatusCode(), 422);
            assertTrue(te.getMessage().matches(".*Job engine version is invalid.*"));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.toString());
        }
    }

    @Test
    public void submitJobWithValidEngineVersionHive()
            throws Exception
    {
        // Valid engine_version must be accepted
        try {
            submitJobWithEngineVersion(TDJob.Type.HIVE, Optional.of(EngineVersion.fromString("stable")));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.toString());
        }
    }

    private void submitJobWithEngineVersion(TDJob.Type type, Optional<TDJob.EngineVersion> engineVersion)
            throws Exception
    {
        TDJobRequestBuilder jobRequestBuilder =
                new TDJobRequestBuilder()
                        .setType(type)
                        .setDatabase("sample_datasets")
                        .setQuery("-- td-client-java test\nselect count(*) cnt from nasdaq");
        jobRequestBuilder = engineVersion.isPresent() ? jobRequestBuilder.setEngineVersion(engineVersion.get()) : jobRequestBuilder;

        TDJobRequest jobRequest = jobRequestBuilder.createTDJobRequest();
        String jobId = client.submit(jobRequest);
        logger.debug("job id: " + jobId);

        TDJobSummary tdJob = waitJobCompletion(jobId);
        TDJob jobInfo = client.jobInfo(jobId);
        logger.debug("job show result: " + tdJob);
        logger.debug("job info: " + jobInfo);
        Optional<String> schema = jobInfo.getResultSchema();
        assertTrue(schema.isPresent());
        assertEquals("[[\"cnt\", \"bigint\"]]", schema.get());

        String[] array = client.jobResult(jobId, TDResultFormat.JSON, input -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                String result = reader.lines().collect(Collectors.joining());
                logger.info("result:\n" + result);
                return objectMapper.readValue(result, String[].class);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, array.length);
        assertEquals(1, jobInfo.getNumRecords());
        assertTrue(Long.parseLong(array[0]) > 0);
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
    public void submitPrestoJobWithPoolName()
            throws Exception
    {
        client.deleteTableIfExists(SAMPLE_DB, "sample_output");
        String poolName = "hadoop2";
        try {
            String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java test\nselect count(*) from nasdaq", null, poolName));
            fail("should not reach here");
        }
        catch (TDClientHttpException e) {
            assertEquals(400, e.getStatusCode());
        }
    }

    @Test
    public void submitPrestoJobWithInvalidPoolName()
            throws Exception
    {
        assertThrows(TDClientHttpException.class, () -> {
            client.deleteTableIfExists(SAMPLE_DB, "sample_output");
            String poolName = "no_such_pool";
            String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "-- td-client-java test\nselect count(*) from nasdaq", null, poolName));
            TDJobSummary tdJob = waitJobCompletion(jobId);
            client.existsTable(SAMPLE_DB, "sample_output");
        }, "Presto resource pool with name 'no_such_pool' does not exist");
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

        String[] array = client.jobResult(jobId, TDResultFormat.JSON, input -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                String result = reader.lines().collect(Collectors.joining());
                logger.info("result:\n" + result);
                return objectMapper.readValue(result, String[].class);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, array.length);
        assertEquals(scheduledTime, Long.parseLong(array[0]));
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

        TDJobSummary statusByDomainKey = client.jobStatusByDomainKey(domainKey);
        assertThat(statusByDomainKey.getJobId(), is(jobId));
    }

    @Test
    public void submitJobWithResultConnectionId()
            throws Exception
    {
        client = mockClient();

        long connectionId = 9321;

        server.enqueue(new MockResponse().setBody("{\"job_id\":\"17\"}"));

        TDJobRequest request = new TDJobRequestBuilder()
                .setType(TDJob.Type.PRESTO)
                .setDatabase("sample_datasets")
                .setQuery("select 1")
                .setResultConnectionId(connectionId)
                .createTDJobRequest();
        String jobId = client.submit(request);
        assertThat(jobId, is("17"));

        RecordedRequest recordedRequest = server.takeRequest();
        String body = URLDecoder.decode(recordedRequest.getBody().readUtf8(), "UTF-8");
        assertThat(body, containsString("result_connection_id=" + connectionId));
    }

    @Test
    public void submitJobWithResultConnectionSettings()
            throws Exception
    {
        client = mockClient();

        String connectionSettings = "{\"type\":\"null\"}";

        server.enqueue(new MockResponse().setBody("{\"job_id\":\"17\"}"));

        TDJobRequest request = new TDJobRequestBuilder()
                .setType(TDJob.Type.PRESTO)
                .setDatabase("sample_datasets")
                .setQuery("select 1")
                .setResultConnectionSettings(connectionSettings)
                .createTDJobRequest();
        String jobId = client.submit(request);
        assertThat(jobId, is("17"));

        RecordedRequest recordedRequest = server.takeRequest();
        String body = URLDecoder.decode(recordedRequest.getBody().readUtf8(), "UTF-8");
        assertThat(body, containsString("result_connection_settings=" + connectionSettings));
    }

    @Test
    public void submitBulkLoadJob()
            throws Exception
    {
        ObjectNode config = JsonNodeFactory.instance.objectNode();
        ObjectNode in = JsonNodeFactory.instance.objectNode();
        in.put("type", "s3");
        config.set("in", in);
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
    public void startSavedQueryByIdMocked()
            throws Exception
    {
        client = mockClient();

        TimeZone utc = TimeZone.getTimeZone("UTC");
        Calendar scheduledTime = GregorianCalendar.getInstance(utc);
        scheduledTime.set(2016, 1, 3, 4, 5, 6);

        server.enqueue(new MockResponse().setBody("{\"id\":\"17\"}"));

        String startedJobId = client.startSavedQuery(4711, scheduledTime.getTime());

        assertThat(startedJobId, is("17"));

        RecordedRequest recordedRequest = server.takeRequest();
        assertThat(recordedRequest.getPath(), is("/v4/queries/4711/jobs"));
        JsonNode requestBody = ObjectMappers.compactMapper().readTree(recordedRequest.getBody().readUtf8());

        assertThat(requestBody.get("scheduled_time").asText(), is("2016-02-03T04:05:06Z"));
    }

    @Test
    public void startSavedQueryWithDomainKey()
            throws Exception
    {
        String domainKey = randomDomainKey();

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
            client.saveQuery(query);

            int epoch1 = 1457046001;
            int epoch2 = epoch1 + 1;

            // Claim the domain key
            TDSavedQueryStartRequest request1 = TDSavedQueryStartRequest.builder()
                    .name(queryName)
                    .scheduledTime(new Date(epoch1 * 1000L))
                    .domainKey(domainKey)
                    .build();
            String jobId = client.startSavedQuery(request1);

            // Attempt to use the same domain key again and verify that we get a conflict
            TDSavedQueryStartRequest request2 = TDSavedQueryStartRequest.builder()
                    .name(queryName)
                    .scheduledTime(new Date(epoch2 * 1000L))
                    .domainKey(domainKey)
                    .build();
            try {
                client.startSavedQuery(request2);
                fail("Expected " + TDClientHttpConflictException.class.getName());
            }
            catch (TDClientHttpConflictException e) {
                assertThat(e.getConflictsWith(), is(Optional.of(jobId)));
            }
        }
        finally {
            client.deleteSavedQuery(queryName);
        }
    }

    @Test
    public void submitExportJob()
            throws Exception
    {
        TDExportJobRequest jobRequest = TDExportJobRequest.builder()
                .database(SAMPLE_DB)
                .table("sample_output")
                .from(new Date(0L))
                .to(new Date(1456522300L * 1000))
                .fileFormat(TDExportFileFormatType.JSONL_GZ)
                .accessKeyId("access key id")
                .secretAccessKey("secret access key")
                .bucketName("bucket")
                .filePrefix("prefix/")
                .poolName(Optional.empty()).build();
        client.createDatabaseIfNotExists(SAMPLE_DB);
        client.createTableIfNotExists(SAMPLE_DB, "sample_output");
        String jobId = client.submitExportJob(jobRequest);
        TDJobSummary tdJob = waitJobCompletion(jobId);
        // this job will do nothing because sample_output table is empty
    }

    @Test
    public void submitExportJobWithDomainKey()
            throws Exception
    {
        String domainKey = randomDomainKey();

        TDExportJobRequest jobRequest = TDExportJobRequest.builder()
                .database(SAMPLE_DB)
                .table("sample_output")
                .from(new Date(0L))
                .to(new Date(1456522300L * 1000))
                .fileFormat(TDExportFileFormatType.JSONL_GZ)
                .accessKeyId("access key id")
                .secretAccessKey("secret access key")
                .bucketName("bucket")
                .filePrefix("prefix/")
                .domainKey(domainKey)
                .build();

        client.createDatabaseIfNotExists(SAMPLE_DB);
        client.createTableIfNotExists(SAMPLE_DB, "sample_output");
        String jobId = client.submitExportJob(jobRequest);

        // Attempt to submit the job again and verify that we get a domain key conflict
        try {
            client.submitExportJob(jobRequest);
            fail("Expected " + TDClientHttpConflictException.class.getName());
        }
        catch (TDClientHttpConflictException e) {
            assertThat(e.getConflictsWith(), is(Optional.of(jobId)));
        }
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
        return Optional.empty();
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

            List<TDColumn> newSchema = new ArrayList<>(targetTable.getSchema());
            newSchema.add(new TDColumn("int_col", TDColumnType.INT, keyName));
            client.updateTableSchema(SAMPLE_DB, t, newSchema);
            TDTable updatedTable = findTable(SAMPLE_DB, t).get();
            logger.debug(updatedTable.toString());
            assertTrue(updatedTable.getSchema().contains(new TDColumn("int_col", TDColumnType.INT, keyName)), "should have updated column");

            // schema test with duplicated key
            newSchema = new ArrayList<>(targetTable.getSchema());
            newSchema.add(new TDColumn("str_col", TDColumnType.STRING, keyName));
            newSchema.add(new TDColumn("str_col", TDColumnType.STRING, keyName));
            client.updateTableSchema(SAMPLE_DB, t, newSchema, true);
            updatedTable = findTable(SAMPLE_DB, t).get();
            logger.debug(updatedTable.toString());
            assertTrue(updatedTable.getSchema().contains(new TDColumn("str_col", TDColumnType.STRING, keyName)), "should have updated column");

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

    @Test
    public void createTableWithoutIdempotentKey()
            throws Exception
    {
        String t = newTemporaryName("non_idempotent_test");
        Assertions.assertThrows(TDClientException.class, () -> {
            try {
                // It should throw TDClientException without idempotent key.
                client.createTable(SAMPLE_DB, t);
                client.createTable(SAMPLE_DB, t);
                Assertions.fail();
            }
            finally {
                logger.info("Deleting table: " + t);
                client.deleteTable(SAMPLE_DB, t);
            }
        });
    }

    @Test
    public void createTableWithIdempotentKey()
            throws Exception
    {
        String t = newTemporaryName("idempotent_test");
        try {
            String idempotentKey = "idempotent_key";
            client.createTable(SAMPLE_DB, t, idempotentKey);
            client.createTable(SAMPLE_DB, t, idempotentKey);
        }
        catch (TDClientException e) {
            // Duplicated request must not throw exception with idempotent key.
            fail();
        }
        finally {
            client.deleteTable(SAMPLE_DB, t);
        }
    }

    private String queryResult(String database, String sql)
            throws InterruptedException
    {
        String jobId = client.submit(TDJobRequest.newPrestoQuery(database, sql));
        waitJobCompletion(jobId);
        return client.jobResult(jobId, TDResultFormat.CSV, input -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
                String result = reader.lines().collect(Collectors.joining());
                logger.info(result);
                return result;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
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
    public void partialDeleteWithDomainKeyTest()
            throws Exception
    {
        String domainKey = randomDomainKey();

        String t = newTemporaryName("td_client_test");
        try {
            client.deleteTableIfExists(SAMPLE_DB, t);

            String jobId = client.submit(TDJobRequest.newPrestoQuery(SAMPLE_DB,
                    String.format("CREATE TABLE %s AS SELECT * FROM (VALUES TD_TIME_PARSE('2015-01-01', 'UTC'), TD_TIME_PARSE('2015-02-01', 'UTC')) as sample(time)", t, t)));

            waitJobCompletion(jobId);

            int from = 1420070400;
            int to = from + 3600;

            TDPartialDeleteJob deleteJob = client.partialDelete(SAMPLE_DB, t, from, to, domainKey);

            try {
                client.partialDelete(SAMPLE_DB, t, from, to, domainKey);
                fail("Expected " + TDClientHttpConflictException.class.getName());
            }
            catch (TDClientHttpConflictException e) {
                assertThat(e.getConflictsWith(), is(Optional.of(deleteJob.getJobId())));
            }
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
            TDBulkImportSession foundInList = sessionList.stream().filter(input -> {
                return input.getName().equals(session);
            }).findAny().get();

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
                try (OutputStream out = new GZIPOutputStream(buf);
                        MessagePacker packer = MessagePack.newDefaultPacker(out)) {
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
                }

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
            BackOff backoff = new ExponentialBackOff();
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

            final AtomicInteger errorRecordCount = new AtomicInteger(0);

            // Error record check
            int errorCount = client.getBulkImportErrorRecords(session, input -> {
                try {
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                    while (unpacker.hasNext()) {
                        Value v = unpacker.unpackValue();
                        logger.info("error record: " + v);
                        errorRecordCount.incrementAndGet();
                    }
                    return errorRecordCount.get();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
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
            TDTable imported = client.listTables(SAMPLE_DB).stream().filter(input -> {
                return input.getName().equals(bulkImportTable);
            }).findFirst().get();

            assertEquals(numRowsInPart * 2, imported.getRowCount());
            List<TDColumn> columns = imported.getColumns();
            logger.info(columns.stream().map(TDColumn::toString).collect(Collectors.joining(", ")));
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

    // disable this as we cannot get a valid credential due to security reason
    public void authenticate()
            throws Exception
    {
        // authenticate() method should retrieve apikey, and set it to the TDClient
        // [NOTE] To pass this you need to add password config to ~/.td/td.conf
        Properties p = TDClientConfig.readTDConf();
        TDClientBuilder clientBuilder = new TDClientBuilder(false); // Set no API key
        String endpoint = p.getProperty("endpoint");
        clientBuilder = (endpoint != null) ? clientBuilder.setEndpoint(endpoint) : clientBuilder; //Set endpoint from td.conf if exists
        TDClient client = clientBuilder.build();
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
        logger.trace("user: {}", user);
    }

    @Test
    public void listUsers()
            throws Exception
    {
        TDUserList userList = client.listUsers();
        for (TDUser user : userList.getUsers()) {
            logger.trace("user: {}", user);
        }
        logger.trace("{} user(s)", userList.getUsers().size());
    }

    @Test
    public void listSavedQuery()
    {
        List<TDSavedQuery> savedQueries = client.listSavedQueries();
        assertTrue(savedQueries.size() > 0);
        logger.info(savedQueries.stream().map(TDSavedQuery::toString).collect(Collectors.joining(", ")));
    }

    @Test
    public void getSavedQueryHistory()
    {
        List<TDSavedQuery> allQueries = client.listSavedQueries();
        List<TDSavedQuery> queries = allQueries.stream()
                .limit(10)
                .collect(Collectors.toList());

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
        return Optional.empty();
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
        //ToDo engine_version never returned.
        // assertEquals(expected.getEngineVersion(), target.getEngineVersion());
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
            assertThat(result.getId(), not(isEmptyOrNullString()));
            Optional<TDSavedQuery> q = findSavedQuery(queryName);
            assertTrue(q.isPresent(), String.format("saved query %s is not found", queryName));

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
            assertThat(updated.getId(), is(result.getId()));
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
        assertFalse(q.isPresent(), String.format("saved query %s should be deleted", queryName));
    }

    @Test
    public void saveQueryWithValidEngineVersion()
    {
        // Save query with valid engine_version. Must be successful.

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
                .setEngineVersion(EngineVersion.fromString("stable"))
                .build();

        try {
            TDSavedQuery result = client.saveQuery(query);
            assertThat(result.getId(), not(isEmptyOrNullString()));
            Optional<TDSavedQuery> q = findSavedQuery(queryName);
            assertTrue(q.isPresent(), String.format("saved query %s is not found", queryName));
        }
        catch (TDClientException e) {
            logger.error("failed", e);
            throw e;
        }
        finally {
            client.deleteSavedQuery(queryName);
        }
        Optional<TDSavedQuery> q = findSavedQuery(queryName);
        assertFalse(q.isPresent(), String.format("saved query %s should be deleted", queryName));
    }

    @Test
    public void saveQueryWithInvalidEngineVersion()
    {
        // Save query with invalid engine_version. Must be error.

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
                .setEngineVersion(TDJob.EngineVersion.fromString("DUMMY_VERSION"))
                .build();

        try {
            TDSavedQuery result = client.saveQuery(query);
            fail("Invalid engine_version must not be accepted");
        }
        catch (TDClientHttpException te) {
            logger.debug(te.toString());
            assertEquals(te.getStatusCode(), 422);
            assertTrue(te.getMessage().matches(".*Engine version is not included.*"));
        }
        catch (TDClientException e) {
            logger.error("failed", e);
            throw e;
        }
        finally {
            Optional<TDSavedQuery> q = findSavedQuery(queryName);
            if (q.isPresent()) {
                client.deleteSavedQuery(queryName);
            }
        }
        Optional<TDSavedQuery> q = findSavedQuery(queryName);
        assertFalse(q.isPresent(), String.format("saved query %s should be deleted", queryName));
    }

    @Test
    public void updateQueryWithValidEngineVersion()
    {
        // Test engine_version update. Updated engine_version is valid one, so must be successful.

        String queryName = newTemporaryName("td_client_test");

        TDSaveQueryRequest query = TDSavedQuery.newBuilder(
                        queryName,
                        TDJob.Type.HIVE,
                        SAMPLE_DB,
                        "select 1",
                        "Asia/Tokyo")
                .setCron("0 * * * *")
                .setPriority(-1)
                .setRetryLimit(2)
                .setEngineVersion(EngineVersion.fromString("stable"))
                .build();

        try {
            TDSavedQuery result = client.saveQuery(query);
            assertThat(result.getId(), not(isEmptyOrNullString()));
            Optional<TDSavedQuery> q = findSavedQuery(queryName);
            assertTrue(q.isPresent(), String.format("saved query %s is not found", queryName));
            // Update
            TDSavedQueryUpdateRequest query2 =
                    TDSavedQuery.newUpdateRequestBuilder()
                            .setQuery("select 2")
                            .setEngineVersion(EngineVersion.fromString("stable"))
                            .build();
            TDSavedQuery updated = client.updateSavedQuery(queryName, query2);
        }
        catch (TDClientException e) {
            logger.error("failed", e);
            throw e;
        }
        finally {
            client.deleteSavedQuery(queryName);
        }
        Optional<TDSavedQuery> q = findSavedQuery(queryName);
        assertFalse(q.isPresent(), String.format("saved query %s should be deleted", queryName));
    }

    @Test
    public void updateQueryWithInvalidEngineVersion()
    {
        // Test engine_version update. Updated engine_version is invalid one, so must be error.

        String queryName = newTemporaryName("td_client_test");

        TDSaveQueryRequest query = TDSavedQuery.newBuilder(
                        queryName,
                        TDJob.Type.HIVE,
                        SAMPLE_DB,
                        "select 1",
                        "Asia/Tokyo")
                .setCron("0 * * * *")
                .setPriority(-1)
                .setRetryLimit(2)
                .setEngineVersion(EngineVersion.fromString("stable"))
                .build();

        try {
            TDSavedQuery result = client.saveQuery(query);
            assertThat(result.getId(), not(isEmptyOrNullString()));
            Optional<TDSavedQuery> q = findSavedQuery(queryName);
            assertTrue(q.isPresent(), String.format("saved query %s is not found", queryName));
            // Update
            TDSavedQueryUpdateRequest query2 =
                    TDSavedQuery.newUpdateRequestBuilder()
                            .setQuery("select 2")
                            .setEngineVersion(TDJob.EngineVersion.fromString("DUMMY_VERSION"))
                            .build();
            TDSavedQuery updated = client.updateSavedQuery(queryName, query2);
            fail("Invalid engine_version must not be accepted");
            logger.error(updated.toString());
        }
        catch (TDClientHttpException te) {
            logger.debug(te.toString());
            assertEquals(te.getStatusCode(), 422);
            assertTrue(te.getMessage().matches(".*Engine version is not included.*"));
        }
        catch (TDClientException e) {
            logger.error("failed", e);
            throw e;
        }
        finally {
            client.deleteSavedQuery(queryName);
        }
        Optional<TDSavedQuery> q = findSavedQuery(queryName);
        assertFalse(q.isPresent(), String.format("saved query %s should be deleted", queryName));
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

    @Test
    public void startBulkLoadSessionJobWithDomainKey()
            throws Exception
    {
        client = mockClient();

        String sessionName = "foobar";
        String expectedJobId = "4711";
        String expectedPath = "/v3/bulk_loads/" + sessionName + "/jobs";
        String domainKey = randomDomainKey();
        String expectedPayload = "{\"domain_key\":\"" + domainKey + "\"}";

        TDBulkLoadSessionStartRequest request = TDBulkLoadSessionStartRequest.builder()
                .setDomainKey(domainKey)
                .build();

        server.enqueue(new MockResponse().setBody("{\"job_id\":\"4711\"}"));

        TDBulkLoadSessionStartResult result = client.startBulkLoadSession(sessionName, request);
        assertThat(result.getJobId(), is(expectedJobId));

        RecordedRequest recordedRequest = server.takeRequest();
        String body = recordedRequest.getBody().readUtf8();
        assertThat(body, is(expectedPayload));
        assertThat(recordedRequest.getPath(), is(expectedPath));
    }

    @Test
    public void lookupConnectionTest()
            throws Exception
    {
        client = mockClient();

        server.enqueue(new MockResponse().setBody("{\"id\":4711}"));

        long id = client.lookupConnection("Please Lookup This!?");
        assertThat(id, is(4711L));

        RecordedRequest recordedRequest = server.takeRequest();
        assertThat(recordedRequest.getPath(), is("/v3/connections/lookup?name=Please%20Lookup%20This!%3F"));
    }

    @Test
    public void lookupConnectionNotFoundTest()
            throws Exception
    {
        try {
            client.lookupConnection("No such connection test");
        }
        catch (TDClientHttpNotFoundException e) {
            // OK
            assertEquals(HttpStatus.NOT_FOUND_404, e.getStatusCode());
        }
    }

    @Test
    public void legacyApikeyMocked()
            throws Exception
    {
        String apikey = String.join("", Collections.nCopies(40, "a"));
        client = mockClient().withApiKey(apikey);

        server.enqueue(new MockResponse());

        client.killJob("4711");

        assertThat(server.getRequestCount(), is(1));
        RecordedRequest recordedRequest = server.takeRequest();

        assertThat(recordedRequest.getPath(), is("/v3/job/kill/4711"));
        assertThat(recordedRequest.getHeader("Authorization"), is("TD1 " + apikey));
    }

    @Test
    public void arbitraryAuthorizationHeaderMocked()
            throws Exception
    {
        String authorization = "Bearer badf00d";
        client = mockClient().withApiKey(authorization);

        server.enqueue(new MockResponse());

        client.killJob("4711");

        assertThat(server.getRequestCount(), is(1));
        RecordedRequest recordedRequest = server.takeRequest();

        assertThat(recordedRequest.getPath(), is("/v3/job/kill/4711"));
        assertThat(recordedRequest.getHeader("Authorization"), is(authorization));
    }

    @Test
    public void arbitraryAuthorizationHeader()
            throws Exception
    {
        String authorization = "TD1 " + apikey();
        client = client.withApiKey(authorization);

        // Verify that the client can submit a job with this apikey configuration
        submitJob();
    }

    @Test
    public void customHeaders()
            throws InterruptedException
    {
        Map<String, Set<String>> headers0 = Collections.singletonMap(
                "k0", Collections.singleton("v0"));
        Map<String, List<String>> headers1 = new HashMap<>();
        headers1.put("k1", Arrays.asList("v1a", "v1b"));
        headers1.put("k2", Collections.singletonList("v2"));
        Map<String, List<String>> headers2 = new HashMap<>();
        headers1.put("k3", Collections.singletonList("v3"));
        headers1.put("k4", Collections.singletonList("v4"));

        client = TDClient.newBuilder(false)
                .setUseSSL(false)
                .setEndpoint(server.getHostName())
                .setPort(server.getPort())
                .setHeaders(headers0)
                .build();

        server.enqueue(new MockResponse());
        server.enqueue(new MockResponse());
        server.enqueue(new MockResponse());

        client.killJob("0");
        client.withHeaders(headers1).killJob("1");
        client.withHeaders(headers1).withHeaders(headers2).killJob("2");

        RecordedRequest request0 = server.takeRequest();
        RecordedRequest request1 = server.takeRequest();
        RecordedRequest request2 = server.takeRequest();

        assertThat(request0.getPath(), is("/v3/job/kill/0"));
        assertThat(request0.getHeaders().toMultimap().get("k0"), containsInAnyOrder("v0"));

        assertThat(request1.getPath(), is("/v3/job/kill/1"));
        assertThat(request1.getHeaders().toMultimap().get("k0"), containsInAnyOrder("v0"));
        assertThat(request1.getHeaders().toMultimap().get("k1"), containsInAnyOrder("v1a", "v1b"));
        assertThat(request1.getHeaders().toMultimap().get("k2"), containsInAnyOrder("v2"));

        assertThat(request2.getPath(), is("/v3/job/kill/2"));
        assertThat(request2.getHeaders().toMultimap().get("k0"), containsInAnyOrder("v0"));
        assertThat(request2.getHeaders().toMultimap().get("k1"), containsInAnyOrder("v1a", "v1b"));
        assertThat(request2.getHeaders().toMultimap().get("k2"), containsInAnyOrder("v2"));
        assertThat(request2.getHeaders().toMultimap().get("k3"), containsInAnyOrder("v3"));
        assertThat(request2.getHeaders().toMultimap().get("k4"), containsInAnyOrder("v4"));
    }

    @Test
    public void submitResultExportJobWithResultOutput()
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"job_id\":\"17\"}"));

        TDExportResultJobRequest jobRequest = TDExportResultJobRequest.builder()
                .jobId("17")
                .resultOutput("td://api_key@/sample_database/sample_output_table")
                .build();

        String jobId = client.submitResultExportJob(jobRequest);
        assertEquals("17", jobId);
    }

    @Test
    public void submitResultExportJobWithConnectionId()
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"job_id\":\"17\"}"));

        TDExportResultJobRequest jobRequest = TDExportResultJobRequest.builder()
                .jobId("17")
                .resultConnectionId("3822")
                .resultConnectionSettings(
                        "{\"api_key\":\"api_key\"," +
                                "\"user_database_name\":\"sample_database\"," +
                                "\"user_table_name\":\"sample_output_table\"}")
                .build();

        String jobId = client.submitResultExportJob(jobRequest);
        assertEquals("17", jobId);
    }

    @Test
    public void submitResultExportJobWithOnlyJobId()
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"job_id\":\"17\"}"));

        TDExportResultJobRequest jobRequest = TDExportResultJobRequest.builder()
                .jobId("17")
                .build();

        Assertions.assertThrows(IllegalStateException.class, () -> {
            client.submitResultExportJob(jobRequest);
        });
    }

    @Test
    public void testTableDistribution()
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"user_table_id\":123,\"bucket_count\":512,\"partition_function\":\"hash\",\"columns\":[{\"key\":\"col1\",\"type\":\"int\",\"name\":\"col1\"},{\"key\":\"col2\",\"type\":\"int\",\"name\":\"col2\"}]}"));

        Optional<TDTableDistribution> distributionOpt = client.tableDistribution("sample_datasets", "www_access");
        assertTrue(distributionOpt.isPresent());
        TDTableDistribution distribution = distributionOpt.get();
        assertEquals(123, distribution.getUserTableId());
        assertEquals(512, distribution.getBucketCount());
        assertEquals("hash", distribution.getPartitionFunction());
        assertEquals(2, distribution.getColumns().size());
        assertEquals("col1", distribution.getColumns().get(0).getName());
        assertEquals("col2", distribution.getColumns().get(1).getName());
    }

    @Test
    public void testMissingTableDistribution()
    {
        Optional<TDTableDistribution> distributionOpt = client.tableDistribution("sample_datasets", "www_access");
        assertFalse(distributionOpt.isPresent());
    }

    @Test
    public void testImportFile()
            throws Exception
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"unique_id\":\"4288048cf8f811e88b560a87157ac806\",\"md5_hex\":\"a34e7c79aa6b6cc48e6e1075c2215a8b\",\"database\":\"db\",\"table\":\"tbl\",\"elapsed_time\":10}"));

        File tmpFile = createTempMsgpackGz("import", 10);
        TDImportResult result = client.importFile("db", "tbl", tmpFile);

        assertEquals(result.getDatabaseName(), "db");
        assertEquals(result.getTableName(), "tbl");
        assertEquals(result.getElapsedTime(), 10);
        assertEquals(result.getMd5Hex(), "a34e7c79aa6b6cc48e6e1075c2215a8b");
        assertEquals(result.getUniqueId(), "4288048cf8f811e88b560a87157ac806");
    }

    @Test
    public void testImportFileWithId()
            throws Exception
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"unique_id\":\"4288048cf8f811e88b560a87157ac806\",\"md5_hex\":\"a34e7c79aa6b6cc48e6e1075c2215a8b\",\"database\":\"db\",\"table\":\"tbl\",\"elapsed_time\":10}"));

        File tmpFile = createTempMsgpackGz("import", 10);
        TDImportResult result = client.importFile("db", "tbl", tmpFile, "4288048cf8f811e88b560a87157ac806");

        assertEquals(result.getDatabaseName(), "db");
        assertEquals(result.getTableName(), "tbl");
        assertEquals(result.getElapsedTime(), 10);
        assertEquals(result.getMd5Hex(), "a34e7c79aa6b6cc48e6e1075c2215a8b");
        assertEquals(result.getUniqueId(), "4288048cf8f811e88b560a87157ac806");
    }

    @Test
    public void testImportBytes()
            throws Exception
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"unique_id\":\"4288048cf8f811e88b560a87157ac806\",\"md5_hex\":\"a34e7c79aa6b6cc48e6e1075c2215a8b\",\"database\":\"db\",\"table\":\"tbl\",\"elapsed_time\":10}"));

        File tmpFile = createTempMsgpackGz("import", 10);
        byte[] bytes = Files.readAllBytes(tmpFile.toPath());
        TDImportResult result = client.importBytes("db", "tbl", bytes);

        assertEquals(result.getDatabaseName(), "db");
        assertEquals(result.getTableName(), "tbl");
        assertEquals(result.getElapsedTime(), 10);
        assertEquals(result.getMd5Hex(), "a34e7c79aa6b6cc48e6e1075c2215a8b");
        assertEquals(result.getUniqueId(), "4288048cf8f811e88b560a87157ac806");
    }

    @Test
    public void testImportBytesWithId()
            throws Exception
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("{\"unique_id\":\"4288048cf8f811e88b560a87157ac806\",\"md5_hex\":\"a34e7c79aa6b6cc48e6e1075c2215a8b\",\"database\":\"db\",\"table\":\"tbl\",\"elapsed_time\":10}"));

        File tmpFile = createTempMsgpackGz("import", 10);
        byte[] bytes = Files.readAllBytes(tmpFile.toPath());
        TDImportResult result = client.importBytes("db", "tbl", bytes, "4288048cf8f811e88b560a87157ac806");

        assertEquals(result.getDatabaseName(), "db");
        assertEquals(result.getTableName(), "tbl");
        assertEquals(result.getElapsedTime(), 10);
        assertEquals(result.getMd5Hex(), "a34e7c79aa6b6cc48e6e1075c2215a8b");
        assertEquals(result.getUniqueId(), "4288048cf8f811e88b560a87157ac806");
    }

    @Test
    public void testGetFederatedQueryConfigsWhenEmpty()
        throws Exception
    {
        client = mockClient();
        server.enqueue(new MockResponse().setBody("[]"));

        List<TDFederatedQueryConfig> result = client.getFederatedQueryConfigs();

        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetFederatedQueryConfigsWhenOnlyOne() throws Exception
    {
        client = mockClient();
        final String federatedQueryConfigs = "[\n" +
            "    {\n" +
            "        \"id\": 12345,\n" +
            "        \"type\": \"custom_type\",\n" +
            "        \"user_id\": 67890,\n" +
            "        \"account_id\": 54321,\n" +
            "        \"name\": \"test_name\",\n" +
            "        \"connection_id\": 112233,\n" +
            "        \"created_at\": \"2023-06-01T00:00:00Z\",\n" +
            "        \"updated_at\": \"2023-06-15T00:00:00Z\",\n" +
            "        \"settings\": {\n" +
            "            \"account_name\": \"snowflake_account\",\n" +
            "            \"auth_method\": \"password\",\n" +
            "            \"private_key\": \"encrypted_key\",\n" +
            "            \"passphrase\": \"secret_phrase\",\n" +
            "            \"options\": {\n" +
            "                \"key1\": \"val1\",\n" +
            "                \"key2\": \"val2\"\n" +
            "            },\n" +
            "            \"user\": \"snowflake_user\",\n" +
            "            \"password\": \"snowflake_password\",\n" +
            "            \"database\": \"sample_database\"\n" +
            "        }\n" +
            "    }\n" +
            "]";
        server.enqueue(new MockResponse().setBody(federatedQueryConfigs));

        List<TDFederatedQueryConfig> result = client.getFederatedQueryConfigs();

        Map<String, Object> settings = new HashMap<>();
        settings.put("account_name", "snowflake_account");
        settings.put("auth_method", "password");
        settings.put("private_key", "encrypted_key");
        settings.put("passphrase", "secret_phrase");
        Map<String, Object> options = new HashMap<>();
        options.put("key1", "val1");
        options.put("key2", "val2");
        settings.put("options", options);
        settings.put("user", "snowflake_user");
        settings.put("password", "snowflake_password");
        settings.put("database", "sample_database");

        List<TDFederatedQueryConfig> expected = Arrays.asList(
            new TDFederatedQueryConfig(12345, "custom_type", 67890, 54321, "test_name", 112233, "2023-06-01T00:00:00Z", "2023-06-15T00:00:00Z", settings)
        );
        assertIterableEquals(result, expected);
    }

    @Test
    public void testGetFederatedQueryConfigsWhenMultiple() throws Exception
    {
        client = mockClient();
        final String federatedQueryConfigs = "[\n" +
            "    {\n" +
            "        \"id\": 12345,\n" +
            "        \"type\": \"custom_type\",\n" +
            "        \"user_id\": 67890,\n" +
            "        \"account_id\": 54321,\n" +
            "        \"name\": \"test_name\",\n" +
            "        \"connection_id\": 112233,\n" +
            "        \"created_at\": \"2023-06-01T00:00:00Z\",\n" +
            "        \"updated_at\": \"2023-06-15T00:00:00Z\",\n" +
            "        \"settings\": {\n" +
            "            \"account_name\": \"snowflake_account\",\n" +
            "            \"auth_method\": \"password\",\n" +
            "            \"private_key\": \"encrypted_key\",\n" +
            "            \"passphrase\": \"secret_phrase\",\n" +
            "            \"options\": {\n" +
            "                \"key1\": \"val1\",\n" +
            "                \"key2\": \"val2\"\n" +
            "            },\n" +
            "            \"user\": \"snowflake_user\",\n" +
            "            \"password\": \"snowflake_password\",\n" +
            "            \"database\": \"sample_database\"\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "        \"id\": 67890,\n" +
            "        \"type\": \"another_type\",\n" +
            "        \"user_id\": 12345,\n" +
            "        \"account_id\": 98765,\n" +
            "        \"name\": \"another_test_name\",\n" +
            "        \"connection_id\": 445566,\n" +
            "        \"created_at\": \"2023-07-01T00:00:00Z\",\n" +
            "        \"updated_at\": \"2023-07-15T00:00:00Z\",\n" +
            "        \"settings\": {\n" +
            "            \"account_name\": \"another_snowflake_account\",\n" +
            "            \"auth_method\": \"oauth\",\n" +
            "            \"private_key\": \"another_encrypted_key\",\n" +
            "            \"passphrase\": \"another_secret_phrase\",\n" +
            "            \"options\": {\n" +
            "                \"keyA\": \"valA\",\n" +
            "                \"keyB\": \"valB\"\n" +
            "            },\n" +
            "            \"user\": \"another_snowflake_user\",\n" +
            "            \"password\": \"another_snowflake_password\",\n" +
            "            \"database\": \"another_sample_database\"\n" +
            "        }\n" +
            "    }\n" +
            "]";
        server.enqueue(new MockResponse().setBody(federatedQueryConfigs));

        List<TDFederatedQueryConfig> result = client.getFederatedQueryConfigs();

        Map<String, Object> settings1 = new HashMap<>();
        settings1.put("account_name", "snowflake_account");
        settings1.put("auth_method", "password");
        settings1.put("private_key", "encrypted_key");
        settings1.put("passphrase", "secret_phrase");
        Map<String, Object> options1 = new HashMap<>();
        options1.put("key1", "val1");
        options1.put("key2", "val2");
        settings1.put("options", options1);
        settings1.put("user", "snowflake_user");
        settings1.put("password", "snowflake_password");
        settings1.put("database", "sample_database");

        Map<String, Object> settings2 = new HashMap<>();
        settings2.put("account_name", "another_snowflake_account");
        settings2.put("auth_method", "oauth");
        settings2.put("private_key", "another_encrypted_key");
        settings2.put("passphrase", "another_secret_phrase");
        Map<String, Object> options2 = new HashMap<>();
        options2.put("keyA", "valA");
        options2.put("keyB", "valB");
        settings2.put("options", options2);
        settings2.put("user", "another_snowflake_user");
        settings2.put("password", "another_snowflake_password");
        settings2.put("database", "another_sample_database");

        List<TDFederatedQueryConfig> expected = Arrays.asList(
            new TDFederatedQueryConfig(12345, "custom_type", 67890, 54321, "test_name", 112233, "2023-06-01T00:00:00Z", "2023-06-15T00:00:00Z", settings1),
            new TDFederatedQueryConfig(67890, "another_type", 12345, 98765, "another_test_name", 445566, "2023-07-01T00:00:00Z", "2023-07-15T00:00:00Z", settings2)
        );
        assertIterableEquals(result, expected);
    }

    @Test
    public void testGetFederatedQueryConfigsWhenAuthenticationFail()
        throws Exception
    {
        TDClient client = TDClient.newBuilder().setApiKey("1/xxfasdfafd").build(); // Set a wrong API key
        server.enqueue(new MockResponse().setBody("[]"));

        try {
            List<TDFederatedQueryConfig> result = client.getFederatedQueryConfigs();
            fail("should not reach here");
        }
        catch (TDClientHttpUnauthorizedException e) {
            // OK
            assertEquals(HttpStatus.UNAUTHORIZED_401, e.getStatusCode());
        }
    }

    private File createTempMsgpackGz(String prefix, int numRows)
            throws IOException
    {
        int count = 0;
        final long time = System.currentTimeMillis() / 1000;

        File tmpFile = File.createTempFile(prefix, ".msgpack.gz");
        try (OutputStream out = new GZIPOutputStream(new FileOutputStream(tmpFile));
                MessagePacker packer = MessagePack.newDefaultPacker(out)) {
            for (int n = 0; n < numRows; ++n) {
                ValueFactory.MapBuilder b = ValueFactory.newMapBuilder();
                b.put(ValueFactory.newString("time"), ValueFactory.newInteger(time + count));
                b.put(ValueFactory.newString("event"), ValueFactory.newString("log" + count));
                b.put(ValueFactory.newString("description"), ValueFactory.newString("sample data"));
                packer.packValue(b.build());
                count += 1;
            }
        }

        return tmpFile;
    }

    private static String apikey()
    {
        Properties props;
        String apikey;

        props = TDClientConfig.readTDConf();
        apikey = props.getProperty("td.client.apikey", props.getProperty("apikey"));
        if (apikey != null) {
            return apikey;
        }

        props = System.getProperties();
        apikey = props.getProperty("td.client.apikey", props.getProperty("apikey"));
        if (apikey != null) {
            return apikey;
        }

        apikey = System.getenv(ENV_TD_CLIENT_APIKEY);
        if (apikey != null) {
            return apikey;
        }

        throw new AssertionError("No apikey found");
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
