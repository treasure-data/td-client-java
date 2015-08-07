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

/**
 * td-client error codes
 */
public enum ErrorCode
{
    AUTHENTICATION_FAILURE(0x001_0001),
    DATABASE_NOT_FOUND(0x001_0002),
    DATABASE_ALREADY_EXISTS(0x0001_0003),
    TABLE_NOT_FOUND(0x001_0004),
    INVALID_CONFIGURATION(0x001_0005),

    API_REQUEST_TIMEOUT(0x0002_0001),
    API_CLIENT_ERROR(0x0002_0002),
    API_EXECUTION_INTERRUPTED(0x0002_0003),
    API_RETRY_LIMIT_EXCEEDED(0x0002_0004),
    API_INVALID_JSON_RESPONSE(0x002_0005),
    API_INVALID_URL(0x0002_0006)

    ;

    private final int code;

    private ErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
