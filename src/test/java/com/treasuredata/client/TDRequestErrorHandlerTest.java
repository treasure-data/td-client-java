package com.treasuredata.client;

import com.treasuredata.client.model.TDApiErrorMessage;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class TDRequestErrorHandlerTest
{
    private static Logger logger = LoggerFactory.getLogger(TestTDHttpClient.class);
    private TDHttpClient client;

    @Before
    public void setUp()
            throws Exception
    {
        client = TDClient.newClient().httpClient;
    }

    @After
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
        Instant retryAfter = new Instant(d);
        Instant expected = new DateTime(1999, 12, 31, 23, 59, 59, DateTimeZone.UTC).toInstant();
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
        Instant retryAfter = new Instant(d);
        Instant expected = new DateTime(now).plusSeconds(120).toInstant();
        assertThat(retryAfter, is(expected));
    }
}
