package com.treasuredata.client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.util.function.Function;

/**
 * Request handler implementations
 */
public class TDHttpRequestHandlers
{
    private TDHttpRequestHandlers()
    {
    }

    public static final TDHttpRequestHandler<String> stringContentHandler = response -> response.body();

    public static final TDHttpRequestHandler<byte[]> byteArrayContentHandler = response -> response.body().getBytes();

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
            try (InputStream inputStream = new ByteArrayInputStream(response.body().getBytes())) {
                return handler.apply(inputStream);
            }
        };
    }

    public static final <Result> TDHttpRequestHandler<Result> newByteStreamHandler(final Function<InputStream, Result> handler)
    {
        return response -> {
            try (InputStream inputStream = new ByteArrayInputStream(response.body().getBytes())) {
                return handler.apply(inputStream);
            }
        };
    }
}
