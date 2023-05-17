package com.treasuredata.client;

import okhttp3.ResponseBody;

import java.io.InputStream;
import java.util.function.Function;

/**
 * Request handler implementations
 */
public class TDHttpRequestHandlers
{
    private TDHttpRequestHandlers()
    {
    }

    public static final TDHttpRequestHandler<String> stringContentHandler = response -> response.body().string();

    public static final TDHttpRequestHandler<byte[]> byteArrayContentHandler = response -> response.body().bytes();

    /**
     * @deprecated Use {@link #newByteStreamHandler(Function)} instead.
     * @param handler
     * @return
     * @param <Result>
     */
    @Deprecated
    public static final <Result> TDHttpRequestHandler<Result> newByteStreamHandler(final com.google.common.base.Function<InputStream, Result> handler)
    {
        return response -> {
            try (ResponseBody body = response.body()) {
                return handler.apply(body.byteStream());
            }
        };
    }

    public static final <Result> TDHttpRequestHandler<Result> newByteStreamHandler(final Function<InputStream, Result> handler)
    {
        return response -> {
            try (ResponseBody body = response.body()) {
                return handler.apply(body.byteStream());
            }
        };
    }
}
