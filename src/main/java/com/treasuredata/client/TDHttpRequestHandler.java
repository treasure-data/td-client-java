package com.treasuredata.client;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;

/**
 *
 */
public interface TDHttpRequestHandler<Result>
{
    /**
     * Set additional request parameters here.
     *
     * @param request
     * @return
     */
    default Request prepareRequest(Request request)
    {
        // Do nothing by default
        return request;
    }

    /**
     * If this returns true, onSuccess(resposne) will be called
     *
     * @param response
     * @return
     */
    default boolean isSuccess(Response response)
    {
        // Just check 200 <= code < 300 range
        return response.isSuccessful();
    }

    /**
     * Send the request through the given client.
     *
     * @param httpClient
     * @param request
     * @return
     * @throws IOException
     */
    default Response send(OkHttpClient httpClient, Request request)
            throws IOException
    {
        return httpClient.newCall(request).execute();
    }

    /**
     * Handle the response
     *
     * @param response
     * @return
     * @throws Exception
     */
    Result onSuccess(Response response)
            throws Exception;

    /**
     * When isSuccess(response) returns false, this method will be called to read the returned response.
     *
     * @param response
     * @return returned content
     */
    default byte[] onError(Response response)
            throws IOException
    {
        try {
            return response.body().bytes();
        }
        catch (IOException e) {
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
    }
}
