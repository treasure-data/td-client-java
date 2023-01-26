package com.treasuredata.client;

import com.google.common.base.Function;
import okhttp3.ResponseBody;

import java.io.InputStream;

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

    public static final <Result> TDHttpRequestHandler<Result> newByteStreamHandler(final Function<InputStream, Result> handler)
    {
        return response -> {
            try (ResponseBody body = response.body()) {
                return handler.apply(body.byteStream());
            }
        };
    }
}
