package com.treasuredata.client;

import com.treasuredata.client.model.TDApiErrorMessage;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
 */
public class TDRequestErrorHandlerTest
{
    private static Logger logger = LoggerFactory.getLogger(TestTDHttpClient.class);
    private TDHttpClient client;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        client = TDClient.newClient().httpClient;
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        client.close();
    }

    @Test
    public void parseInvalidErrorMessage()
    {
        Response response = new Response.Builder()
                .request(new Request.Builder().url("https://api.treasuredata.com/v3/server_status").build())
                .message("")
                .code(HttpStatus.ACCEPTED_202)
                .protocol(Protocol.HTTP_1_1)
                .body(ResponseBody.create(MediaType.parse("application/json"), "{invalid json response}"))
                .build();

        Optional<TDApiErrorMessage> err = TDRequestErrorHandler.extractErrorResponse(response);
        assertFalse(err.isPresent());
    }

    @Test
    public void testParseRetryAfterHttpDate()
            throws Exception
    {
        Response response = new Response.Builder()
                .request(new Request.Builder().url("https://api.treasuredata.com/v3/server_status").build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("")
                .headers(Headers.of("Retry-After", "Fri, 31 Dec 1999 23:59:59 GMT"))
                .build();

        long now = System.currentTimeMillis();
        Date d = TDRequestErrorHandler.parseRetryAfter(now, response);
        Instant retryAfter = d.toInstant();
        Instant expected = Instant.parse("1999-12-31T23:59:59Z");
        assertThat(retryAfter, is(expected));
    }

    @Test
    public void testParseRetryAfterSeconds()
            throws Exception
    {
        Response response = new Response.Builder()
                .request(new Request.Builder().url("https://api.treasuredata.com/v3/server_status").build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("")
                .headers(Headers.of("Retry-After", "120"))
                .build();

        long now = System.currentTimeMillis();

        Date d = TDRequestErrorHandler.parseRetryAfter(now, response);
        Instant retryAfter = d.toInstant();
        Instant expected = Instant.ofEpochMilli(now).plusSeconds(120);
        assertThat(retryAfter, is(expected));
    }

    @Test
    public void defaultExceptionResolverBrokenPipe()
    {
        SocketException socketException = new SocketException("Broken pipe");
        TDClientException result = TDRequestErrorHandler.defaultExceptionResolver(socketException);
        assertThat(result, instanceOf(TDClientSocketException.class));
    }

    @Test
    public void defaultExceptionResolverConnectionReset()
    {
        SocketException socketException = new SocketException("Connection reset");
        TDClientException result = TDRequestErrorHandler.defaultExceptionResolver(socketException);
        assertThat(result, instanceOf(TDClientSocketException.class));
    }

    @Test
    public void defaultExceptionResolverSocketClosed()
    {
        SocketException socketException = new SocketException("Socket closed");
        TDClientException result = TDRequestErrorHandler.defaultExceptionResolver(socketException);
        assertThat(result, instanceOf(TDClientSocketException.class));
    }

    @Test
    public void defaultExceptionResolverUnknownSocketException()
    {
        SocketException socketException = new SocketException("Unknown socket error");
        assertThrows(TDClientSocketException.class, () -> TDRequestErrorHandler.defaultExceptionResolver(socketException));
    }
}
