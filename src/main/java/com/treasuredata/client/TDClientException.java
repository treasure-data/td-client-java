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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Exception class for reporting td-client errors
 */
public class TDClientException
        extends RuntimeException
{
    /**
     * Used for showing detailed error message
     */
    public static enum ErrorType
    {
        // Configuration error
        INVALID_CONFIGURATION,
        INVALID_INPUT,
        // HTTP error
        AUTHENTICATION_FAILURE,
        TARGET_NOT_FOUND,
        TARGET_ALREADY_EXISTS,
        SERVER_ERROR,
        CLIENT_ERROR,
        // Proxy error
        PROXY_AUTHENTICATION_FAILURE,
        // Request handling error
        INTERRUPTED,
        REQUEST_TIMEOUT,
        SSL_ERROR,
        EXECUTION_FAILURE,
        UNEXPECTED_RESPONSE_CODE,
        INVALID_JSON_RESPONSE,
        SOCKET_ERROR;
    }

    private final ErrorType errorType;
    private final Optional<Exception> rootCause;

    private static final String formatErrorMessage(ErrorType errorType, String message, Optional<Exception> cause)
    {
        String rootCauseErrorMessage = cause.isPresent() ? " The root cause: " + cause.get().getMessage() : "";
        return String.format("[%s] %s%s", errorType.name(), message != null ? message : "", rootCauseErrorMessage);
    }

    public TDClientException(ErrorType errorType, String message, Optional<Exception> cause)
    {
        super(formatErrorMessage(errorType, message, cause), cause.orElse(null));
        this.errorType = requireNonNull(errorType, "errorType is null");
        this.rootCause = requireNonNull(cause, "cause is null");
    }

    public TDClientException(ErrorType errorType, String message, Exception cause)
    {
        this(errorType, message, Optional.of(cause));
    }

    public TDClientException(ErrorType errorType, Exception cause)
    {
        this(errorType, cause.getMessage(), Optional.of(cause));
    }

    public TDClientException(ErrorType errorType, String message)
    {
        this(errorType, message, Optional.empty());
    }

    public ErrorType getErrorType()
    {
        return this.errorType;
    }

    public Optional<Exception> getRootCause()
    {
        return rootCause;
    }
}
