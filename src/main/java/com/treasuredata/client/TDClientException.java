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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Exception class for reporting td-client errors
 */
public class TDClientException extends RuntimeException
{
    /**
     * Used for showing detailed error message
     */
    public static enum ErrorType
    {
        // Configuration error
        INVALID_CONFIGURATION,

        // HTTP error
        AUTHENTICATION_FAILURE,
        TARGET_NOT_FOUND,
        TARGET_ALREADY_EXISTS,
        SERVER_ERROR,
        CLIENT_ERROR,

        // Request handling error
        INTERRUPTED,
        REQUEST_TIMEOUT,
        EXECUTION_FAILURE,
        UNEXPECTED_RESPONSE_CODE,
        INVALID_JSON_RESPONSE,
        RESPONSE_READ_FAILURE,
        ;
    }

    private final ErrorType errorType;
    private final Optional<Exception> rootCause;

    private static final String formatErrorMessage(ErrorType errorType, String message, Optional<Exception> cause) {
        String rootCauseErrorMessage = cause.isPresent()?  " The root cause: " + cause.get().getMessage() : "";
        return String.format("[%s] %s%s", errorType.name(), message != null ? message : "", rootCauseErrorMessage);
    }

    public TDClientException(ErrorType errorType, String message, Optional<Exception> cause)
    {
        super(formatErrorMessage(errorType, message, cause));
        checkNotNull(errorType, "errorType is null");
        checkNotNull(cause, "cause is null");
        this.errorType = errorType;;
        this.rootCause = cause;
    }

    public TDClientException(ErrorType errorType, Exception cause) {
        this(errorType, cause.getMessage(), Optional.of(cause));
    }

    public TDClientException(ErrorType errorType, String message) {
        this(errorType, message, Optional.<Exception>absent());
    }

    public ErrorType getErrorType() {
        return this.errorType;
    }

    public Optional<Exception> getRootCause() {
        return rootCause;
    }
}
