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

import java.util.Date;

/**
 * Exception class for reporting http server status code
 */
public class TDClientHttpException
        extends TDClientException
{
    private final int statusCode;

    private final long retryAfter;

    public TDClientHttpException(ErrorType errorType, String message, int statusCode, Date retryAfter)
    {
        super(errorType, message);
        this.statusCode = statusCode;
        this.retryAfter = retryAfter == null ? -1 : retryAfter.getTime();
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public Optional<Date> getRetryAfter()
    {
        if (retryAfter == -1) {
            return Optional.absent();
        }
        else {
            return Optional.of(new Date(retryAfter));
        }
    }
}
