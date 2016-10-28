package com.treasuredata.client;

import java.util.Date;

/**
 * 429 Too Many Requests error (i.e., rate limited).
 */

public class TDClientHttpTooManyRequestsException
        extends TDClientHttpException
{
    public static final int TOO_MANY_REQUESTS_429 = 429;

    public TDClientHttpTooManyRequestsException(String errorMessage, Date retryAfter)
    {
        super(ErrorType.CLIENT_ERROR, errorMessage, TOO_MANY_REQUESTS_429, retryAfter);
    }
}
