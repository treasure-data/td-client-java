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
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.treasuredata.client.model.TDAuthenticationResult;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobList;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobStatus;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDTable;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

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
    public void serverStatus()
    {
        String status = client.serverStatus();
        logger.info(status);
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

        for (TDTable t : tableList) {
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
    }

    @Test
    public void submitJob()
            throws Exception
    {
        String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "select count(*) from nasdaq"));
        logger.debug("job id: " + jobId);

        int retryCount = 0;

        TDJobStatus tdJob = null;
        do {
            Thread.sleep(1000);
            tdJob = client.jobStatus(jobId);
            logger.debug("job status: " + tdJob);
            retryCount++;
        }
        while (retryCount < 10 && !tdJob.getStatus().isFinished());

        TDJob jobInfo = client.jobInfo(jobId);
        logger.debug("job show result: " + tdJob);

        JSONArray array = client.jobResult(jobId, TDResultFormat.JSON, new Function<InputStream, JSONArray>()
        {
            @Override
            public JSONArray apply(InputStream input)
            {
                try {
                    String result = new String(ByteStreams.toByteArray(input));
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
    }

    @Test
    public void invalidJobStatus()
    {
        try {
            TDJobStatus invalidJob = client.jobStatus("xxxxxx");
            logger.debug("invalid job: " + invalidJob);

            fail("should not reach here");
        }
        catch (TDClientException e) {
            assertEquals(TDClientException.ErrorType.TARGET_NOT_FOUND, e.getErrorType());
        }
    }

    private static String SAMPLE_DB = "_tdclient_test";
    private static String SAMPLE_TABLE = "sample";

    @Test
    public void createAndDeleteDatabase()
            throws Exception
    {
        client.createDatabaseIfNotExists(SAMPLE_DB);
        client.deleteDatabase(SAMPLE_DB);
    }

    @Test
    public void tableOperation()
            throws Exception
    {
        client.deleteDatabaseIfExists(SAMPLE_DB);
        client.createDatabaseIfNotExists(SAMPLE_DB);

        client.deleteTableIfExists(SAMPLE_DB, SAMPLE_TABLE);

        client.createTable(SAMPLE_DB, SAMPLE_TABLE);
        client.deleteTable(SAMPLE_DB, SAMPLE_TABLE);

        client.createTableIfNotExists(SAMPLE_DB, SAMPLE_TABLE);

        // rename
        String newTableName = SAMPLE_TABLE + "_renamed";
        client.deleteTableIfExists(SAMPLE_DB, newTableName);
        client.renameTable(SAMPLE_DB, SAMPLE_TABLE, newTableName);
        assertTrue(client.existsTable(SAMPLE_DB, newTableName));
        assertFalse(client.existsTable(SAMPLE_DB, SAMPLE_TABLE));
    }

    @Ignore
    @Test
    public void testBuilkImport()
    {
        // TODO
    }

    @Test
    public void authenticate()
            throws Exception
    {
        Properties p = TDClientConfig.readTDConf();
        TDClient client = new TDClient(new TDClientConfig.Builder().result()); // Set no API key
        String user = firstNonNull(p.getProperty("user"), System.getenv().get("TD_USER"));
        String password = firstNonNull(p.getProperty("password"), System.getenv().get("TD_PASS"));
        TDAuthenticationResult result = client.authenticate(user, password);
        List<TDTable> tableList = client.listTables("sample_datasets");
        assertTrue(tableList.size() >= 2);
    }
}
