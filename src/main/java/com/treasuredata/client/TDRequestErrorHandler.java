package com.treasuredata.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.treasuredata.client.model.TDApiErrorMessage;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;

import java.io.EOFException;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.ProtocolException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.net.HttpHeaders.RETRY_AFTER;
import static com.treasuredata.client.TDClientException.ErrorType.CLIENT_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_INPUT;
import static com.treasuredata.client.TDClientException.ErrorType.INVALID_JSON_RESPONSE;
import static com.treasuredata.client.TDClientException.ErrorType.PROXY_AUTHENTICATION_FAILURE;
import static com.treasuredata.client.TDClientException.ErrorType.SERVER_ERROR;
import static com.treasuredata.client.TDClientException.ErrorType.UNEXPECTED_RESPONSE_CODE;
import static com.treasuredata.client.TDClientHttpTooManyRequestsException.TOO_MANY_REQUESTS_429;
import static com.treasuredata.client.TDHttpRequestHandler.ResponseContext;

/**
 * TDRequestErrorHandler has a logic to handle http request retris,
 */
public class TDRequestErrorHandler
{
    private TDRequestErrorHandler()
    {
    }

    // Use TDHttpClient's logger for better log messages
    private static Logger logger = LoggerFactory.getLogger(TDHttpClient.class);

    @VisibleForTesting
    static final ThreadLocal<SimpleDateFormat> HTTP_DATE_FORMAT = new ThreadLocal<SimpleDateFormat>()
    {
        @Override
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
        }
    };

    /**
     * Show or suppress warning messages for TDClientHttpException
     */
    private static TDClientHttpException clientError(TDClientHttpException e, ResponseContext responseContext) {
        boolean showWarning = true;
        boolean showStackTrace = true;
        switch(e.getStatusCode()) {
            case HttpStatus.NOT_FOUND_404:
                if(responseContext.apiRequest.getPath().startsWith("/v3/table/distribution")) {
                    // Table distribution data will not be found for non-UDP tables.
                    showWarning = false;
                }
                break;
            case HttpStatus.CONFLICT_409:
                // Suppress stack trace because 409 frequently happens when checking the presence of databases and tables.
                showStackTrace = false;
                break;
        }
        if(showWarning) {
            if(showStackTrace) {
                logger.warn(String.format("API request to %s failed: %s, cause: %s", responseContext.apiRequest.getPath(), e.getClass(), e.getCause() == null ? e.getMessage() : e.getCause().getClass()), e);
            }
            else {
                logger.warn(String.format("API request to %s failed: %s, cause: %s", responseContext.apiRequest.getPath(), e.getClass(), e.getCause() == null ? e.getMessage() : e.getCause().getClass()));
            }
        }
        return e;
    }

    public static TDClientException defaultHttpResponseErrorResolver(ResponseContext responseContext)
            throws TDClientException
    {
        Response response = responseContext.response;
        int code = response.code();
        long now = System.currentTimeMillis();

        Date retryAfter = parseRetryAfter(now, response);
        Optional<TDApiErrorMessage> errorResponse = extractErrorResponse(responseContext.response);
        String responseErrorText = errorResponse.isPresent() ? ": " + errorResponse.get().getText() : "";
        String errorMessage = String.format("[%d:%s] API request to %s has failed%s", code, HttpStatus.getMessage(code), responseContext.apiRequest.getPath(), responseErrorText);
        if (HttpStatus.isClientError(code)) {
            logger.debug(errorMessage);
            switch (code) {
                // Soft 4xx errors. These we retry.
                case TOO_MANY_REQUESTS_429:
                    return clientError(new TDClientHttpTooManyRequestsException(errorMessage, retryAfter), responseContext);
                // Hard 4xx error. We do not retry the execution on this type of error
                case HttpStatus.UNAUTHORIZED_401:
                    throw clientError(new TDClientHttpUnauthorizedException(errorMessage), responseContext);
                case HttpStatus.NOT_FOUND_404:
                    throw clientError(new TDClientHttpNotFoundException(errorMessage), responseContext);
                case HttpStatus.CONFLICT_409:
                    String conflictsWith = errorResponse.isPresent() ? parseConflictsWith(errorResponse.get()) : null;
                    throw clientError(new TDClientHttpConflictException(errorMessage, conflictsWith), responseContext);
                case HttpStatus.PROXY_AUTHENTICATION_REQUIRED_407:
                    throw clientError(new TDClientHttpException(PROXY_AUTHENTICATION_FAILURE, errorMessage, code, retryAfter), responseContext);
                case HttpStatus.UNPROCESSABLE_ENTITY_422:
                    throw clientError(new TDClientHttpException(INVALID_INPUT, errorMessage, code, retryAfter), responseContext);
                default:
                    throw clientError(new TDClientHttpException(CLIENT_ERROR, errorMessage, code, retryAfter), responseContext);
            }
        }
        logger.warn(errorMessage);
        if (HttpStatus.isServerError(code)) {
            // Just returns exception info for 5xx errors
            return clientError(new TDClientHttpException(SERVER_ERROR, errorMessage, code, retryAfter), responseContext);
        }
        else {
            throw clientError(new TDClientHttpException(UNEXPECTED_RESPONSE_CODE, errorMessage, code, retryAfter), responseContext);
        }
    }

    public static TDClientException defaultErrorResolver(Throwable e)
            throws TDClientException
    {
        if (e instanceof Exception) {
            return defaultExceptionResolver((Exception) e);
        }
        else {
            throw new TDClientProcessingException(new RuntimeException(e));
        }
    }

    /**
     * @return If the error type is retryable, return the exception. If not, throw it as TDClientException
     * @throws TDClientException
     */
    public static TDClientException defaultExceptionResolver(Exception e)
            throws TDClientException
    {
        if (TDClientException.class.isAssignableFrom(e.getClass())) {
            // If the error is known error, we should throw it as is
            throw (TDClientException) e;
        }
        else if (e instanceof ProtocolException || e instanceof ConnectException || e instanceof EOFException) {
            // OkHttp throws ProtocolException the content length is insufficient
            // ConnectionException can be throw if server is shutting down
            // EOFException can be thrown when the connection was interrupted
            return new TDClientInterruptedException("connection failure", e);
        }
        else if (e instanceof TimeoutException || e instanceof SocketTimeoutException) {
            // OkHttp throws SocketTimeoutException
            return new TDClientTimeoutException(e);
        }
        else if (e instanceof SocketException) {
            final SocketException socketException = (SocketException) e;
            if (socketException instanceof BindException ||
                    socketException instanceof ConnectException ||
                    socketException instanceof NoRouteToHostException ||
                    socketException instanceof PortUnreachableException) {
                // All known SocketException are retryable.
                return new TDClientSocketException(socketException);
            }
            else {
                // Other unknown SocketException are considered non-retryable.
                throw new TDClientSocketException(socketException);
            }
        }
        else if (e instanceof SSLException) {
            SSLException sslException = (SSLException) e;
            if (sslException instanceof SSLHandshakeException || sslException instanceof SSLKeyException || sslException instanceof SSLPeerUnverifiedException) {
                // deterministic SSL exceptions
                throw new TDClientSSLException(sslException);
            }
            else {
                // SSLProtocolException and uncategorized SSL exceptions (SSLException) such as unexpected_message may be retryable
                return new TDClientSSLException(sslException);
            }
        }
        else if (e.getCause() != null && Exception.class.isAssignableFrom(e.getCause().getClass())) {
            return defaultExceptionResolver((Exception) e.getCause());
        }
        else {
            logger.warn("unknown type exception: " + e.getClass(), e);
            throw new TDClientProcessingException(e);
        }
    }

    /**
     * https://tools.ietf.org/html/rfc7231#section-7.1.3
     */
    @VisibleForTesting
    static Date parseRetryAfter(long now, Response response)
    {
        String retryAfter = response.header(RETRY_AFTER);
        if (retryAfter == null) {
            return null;
        }
        // Try parsing as a number of seconds first
        try {
            long retryAfterSeconds = Long.parseLong(retryAfter);
            return new Date(now + TimeUnit.SECONDS.toMillis(retryAfterSeconds));
        }
        catch (NumberFormatException e) {
            // Then try parsing as a HTTP-date
            try {
                return HTTP_DATE_FORMAT.get().parse(retryAfter);
            }
            catch (ParseException ignore) {
                logger.warn("Failed to parse Retry-After header: '" + retryAfter + "'");
                return null;
            }
        }
    }

    private static String parseConflictsWith(TDApiErrorMessage errorResponse)
    {
        Map<String, Object> details = errorResponse.getDetails();
        if (details == null) {
            return null;
        }
        Object conflictsWith = details.get("conflicts_with");
        if (conflictsWith == null) {
            return null;
        }
        return String.valueOf(conflictsWith);
    }

    @VisibleForTesting
    public static Optional<TDApiErrorMessage> extractErrorResponse(Response response)
    {
        Optional<String> content = Optional.absent();
        try {
            try {
                content = Optional.of(response.body().string());
            }
            catch (IOException e) {
                throw new TDClientException(INVALID_JSON_RESPONSE, e);
            }

            if (content.isPresent() && content.get().length() > 0 && content.get().charAt(0) == '{') {
                // Error message from TD API
                return Optional.of(TDHttpClient.defaultObjectMapper.readValue(content.get(), TDApiErrorMessage.class));
            }
            else {
                // Error message from Proxy server etc.
                return Optional.of(new TDApiErrorMessage("error", content.or("[empty]"), "error"));
            }
        }
        catch (IOException e) {
            logger.warn("Failed to parse the error response {}: {}\n{}", response.request().url(), content.or("[empty]"), e.getMessage());
        }
        return Optional.absent();
    }
}
