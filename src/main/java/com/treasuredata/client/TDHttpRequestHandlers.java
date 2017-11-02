package com.treasuredata.client;

import com.google.common.base.Function;
import okhttp3.Response;
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

    public static final TDHttpRequestHandler<String> stringContentHandler = new TDHttpRequestHandler<String>()
    {
        @Override
        public String onSuccess(Response response)
                throws Exception
        {
            return response.body().string();
        }
    };

    public static final TDHttpRequestHandler<byte[]> byteArrayContentHandler = new TDHttpRequestHandler<byte[]>()
    {
        @Override
        public byte[] onSuccess(Response response)
                throws Exception
        {
            return response.body().bytes();
        }
    };

    public static final <Result> TDHttpRequestHandler<Result> newByteStreamHandler(final Function<InputStream, Result> handler)
    {
        return new TDHttpRequestHandler<Result>()
        {
            @Override
            public Result onSuccess(Response response)
                    throws Exception
            {
                try (ResponseBody body = response.body()) {
                    return handler.apply(body.byteStream());
                }
            }
        };
    }
}
