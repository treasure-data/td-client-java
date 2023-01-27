package com.treasuredata.client;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;
import static com.treasuredata.client.TDRequestErrorHandler.defaultErrorResolver;
import static com.treasuredata.client.TDRequestErrorHandler.defaultHttpResponseErrorResolver;

/**
 *
 */
@FunctionalInterface
public interface TDHttpRequestHandler<Result>
{
    static class ResponseContext
    {
        public final TDApiRequest apiRequest;
        public final Response response;

        public ResponseContext(TDApiRequest apiRequest, Response response)
        {
            this.apiRequest = apiRequest;
            this.response = response;
        }
    }

    /**
     * Set additional request parameters here.
     */
    default Request prepareRequest(Request request)
    {
        // Do nothing by default
        return request;
    }

    /**
     * If this returns true, onSuccess(resposne) will be called
     */
    default boolean isSuccess(ResponseContext responseContext)
    {
        // Just check 200 <= code < 300 range
        return responseContext.response.isSuccessful();
    }

    /**
     * Send the request through the given client.
     * @throws IOException
     */
    default Response send(OkHttpClient httpClient, Request request)
            throws IOException
    {
        return httpClient.newCall(request).execute();
    }

    /**
     * Handle the response
     * @throws Exception
     */
    Result onSuccess(Response response)
            throws Exception;

    /**
     * Resolve a corresponding TDClientException for the error response
     *
     * @param responseContext
     * @return
     * @throws TDClientException
     */
    default TDClientException resolveHttpResponseError(ResponseContext responseContext)
            throws TDClientException
    {
        return defaultHttpResponseErrorResolver(responseContext);
    }

    /**
     * Resolve a corresponding TDClientException for the exception thrown while receiving a response.
     *
     * @param e
     * @return
     * @throws TDClientException
     */
    default TDClientException resolveError(Throwable e)
            throws TDClientException
    {
        return defaultErrorResolver(e);
    }

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
