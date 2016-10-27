package com.treasuredata.client;

import com.google.common.base.Optional;

/**
 * 429 Too Many Requests error (i.e., rate limited).
 */

public class TDClientHttpTooManyRequestsException
        extends TDClientHttpException
{
    public static final int TOO_MANY_REQUESTS_429 = 429;

    private final Optional<Long> retryAfterSeconds;

    public TDClientHttpTooManyRequestsException(String errorMessage, Optional<Long> retryAfterSeconds)
    {
        super(ErrorType.CLIENT_ERROR, errorMessage, TOO_MANY_REQUESTS_429);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public Optional<Long> getRetryAfterSeconds()
    {
        return retryAfterSeconds;
    }
}
