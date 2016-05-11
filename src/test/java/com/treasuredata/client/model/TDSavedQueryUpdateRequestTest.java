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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.treasuredata.client.model.TDSavedQuery.newUpdateRequestBuilder;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TDSavedQueryUpdateRequestTest
{
    private static final Logger logger = LoggerFactory.getLogger(TDSavedQueryUpdateRequest.class);

    private static void validateRoundTrip(TDSavedQueryUpdateRequest original)
            throws IOException
    {
        String json = original.toJson();
        logger.trace("request: {}", original);
        logger.debug("json: {}", json);

        TDSavedQueryUpdateRequest read = TDSavedQueryUpdateRequest.getObjectMapper().readValue(json, TDSavedQueryUpdateRequest.class);
        logger.debug("read json: {}", read.toJson());
        assertEquals(original, read);
    }

    @Test
    public void jsonFormat()
            throws IOException
    {
        validateRoundTrip(newUpdateRequestBuilder()
                .setName("new_job_name")
                .build());

        validateRoundTrip(newUpdateRequestBuilder()
                .setQuery("select 1")
                .build());

        validateRoundTrip(newUpdateRequestBuilder()
                .setCron("0 * * * *")
                .build());

        validateRoundTrip(newUpdateRequestBuilder()
                .setDatabase("sample_datasets")
                .build());

        validateRoundTrip(newUpdateRequestBuilder()
                .setTimezone("UTC")
                .build());

        validateRoundTrip(newUpdateRequestBuilder()
                .setDelay(10)
                .build());

        validateRoundTrip(newUpdateRequestBuilder()
                .setResult("td://xxx/yyyy")
                .build());

        validateRoundTrip(newUpdateRequestBuilder()
                .setRetryLimit(3)
                .build());

        for (TDJob.Type t : TDJob.Type.values()) {
            validateRoundTrip(newUpdateRequestBuilder()
                    .setType(t)
                    .build());
        }
    }

    @Test
    public void multipleParamTest()
            throws IOException
    {
        validateRoundTrip(
                TDSavedQuery.newUpdateRequestBuilder()
                        .setCron("15 * * * *")
                        .setType(TDJob.Type.HIVE)
                        .setQuery("select 2")
                        .setTimezone("UTC")
                        .setDelay(20)
                        .setDatabase("sample_db")
                        .setPriority(-1)
                        .setRetryLimit(2)
                        .setResult("mysql://testuser2:pass@somemysql.address/somedb2/sometable2")
                        .build());

        validateRoundTrip(
                TDSavedQuery.newUpdateRequestBuilder()
                        .setName("new_job_name")
                        .setType(TDJob.Type.PRESTO)
                        .setQuery("select 3")
                        .setTimezone("UTC")
                        .setDatabase("sample_db")
                        .build());
    }
}
