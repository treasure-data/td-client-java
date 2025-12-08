package com.treasuredata.client;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
        public final HttpResponse<String> response;

        public ResponseContext(TDApiRequest apiRequest, HttpResponse<String> response)
        {
            this.apiRequest = apiRequest;
            this.response = response;
        }
    }

    /**
     * Set additional request parameters here.
     */
    default HttpRequest.Builder prepareRequest(HttpRequest.Builder requestBuilder)
    {
        // Do nothing by default
        return requestBuilder;
    }

    /**
     * If this returns true, onSuccess(resposne) will be called
     */
    default boolean isSuccess(ResponseContext responseContext)
    {
        // Just check 200 <= code < 300 range
        int statusCode = responseContext.response.statusCode();
        return statusCode >= 200 && statusCode < 300;
    }

    /**
     * Send the request through the given client.
     * @throws IOException
     */
    default HttpResponse<String> send(HttpClient httpClient, HttpRequest request)
            throws IOException, InterruptedException
    {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Handle the response
     * @throws Exception
     */
    Result onSuccess(HttpResponse<String> response)
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
    default byte[] onError(HttpResponse<String> response)
            throws IOException
    {
        try {
            return response.body().getBytes();
        }
        catch (Exception e) {
            throw new TDClientException(INVALID_JSON_RESPONSE, e);
        }
    }
}
