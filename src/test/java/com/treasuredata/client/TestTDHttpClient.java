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

import com.google.common.base.Optional;
import com.treasuredata.client.model.TDApiErrorMessage;
import org.eclipse.jetty.client.api.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;

/**
 *
 */
public class TestTDHttpClient
{
    private static Logger logger = LoggerFactory.getLogger(TestTDHttpClient.class);
    private TDHttpClient client;

    @Before
    public void setUp()
            throws Exception
    {
        client = new TDHttpClient(TDClientConfig.currentConfig());
    }

    @After
    public void tearDown()
            throws Exception
    {
        client.close();
    }

    @Test
    public void parseInvalidErrorMessage()
    {
        Optional<TDApiErrorMessage> err = client.parseErrorResponse("{invalid json response}".getBytes(StandardCharsets.UTF_8));
        assertFalse(err.isPresent());
    }

    @Test
    public void addHttpRequestHeader()
    {
        TDApiRequest req = TDApiRequest.Builder.GET("/v3/system/server_status").addHeader("TEST_HEADER", "hello td-client-java").build();
        Response resp = client.submitRequest(req, Optional.<String>absent(), new TDHttpClient.DefaultContentHandler());
    }

    @Test
    public void deleteMethodTest()
    {
        try {
            TDApiRequest req = TDApiRequest.Builder.DELETE("/v3/dummy_endpoint").build();
            Response resp = client.submitRequest(req, Optional.<String>absent(), new TDHttpClient.DefaultContentHandler());
        }
        catch (TDClientHttpException e) {
            logger.warn("error", e);
        }
    }
}
