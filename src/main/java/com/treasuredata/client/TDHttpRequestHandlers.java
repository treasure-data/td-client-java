package com.treasuredata.client;

import java.io.ByteArrayInputStream;
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

    public static final TDHttpRequestHandler<String> stringContentHandler = response -> response.body();

    public static final TDHttpRequestHandler<byte[]> byteArrayContentHandler = response -> response.body().getBytes();

    public static final <Result> TDHttpRequestHandler<Result> newByteStreamHandler(final Function<InputStream, Result> handler)
    {
        return response -> {
            try (InputStream inputStream = new ByteArrayInputStream(response.body().getBytes())) {
                return handler.apply(inputStream);
            }
        };
    }
}
