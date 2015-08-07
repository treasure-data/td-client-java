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

/**
 * Exception class for reporting td-client errors
 */
public class TDClientException extends Exception
{
    private final ErrorCode errorCode;
    private final Optional<Exception> rootCause;

    private static final String formatErrorMessage(ErrorCode errorCode, String message) {
        return String.format("[%s] %s", errorCode.name(), message);
    }

    public TDClientException(ErrorCode errorCode, String message, Optional<Exception> cause)
    {
        super(formatErrorMessage(errorCode, message));
        this.errorCode = errorCode;;
        this.rootCause = cause;
    }

    public TDClientException(ErrorCode errorCode, Exception cause) {
        this(errorCode, cause.getMessage(), Optional.of(cause));
    }

    public TDClientException(ErrorCode errorCode, String message) {
        this(errorCode, message, Optional.<Exception>absent());
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    public Optional<Exception> getRootCause() {
        return rootCause;
    }
}
