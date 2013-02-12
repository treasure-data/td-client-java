package com.treasure_data.client;

@SuppressWarnings("serial")
public class HttpClientException extends ClientException {
    public static String toMessage(String reason, String message, int code) {
        return String.format("%s, response message = %s, code = %d", reason, message, code);
    }

    private String responseMessage;
    private int responseCode;

    public HttpClientException(String reason, String message, int code) {
        super(toMessage(reason, message, code));
        responseMessage = message;
        responseCode = code;
    }

    public HttpClientException(String reason, String message, int code,
            Throwable cause) {
        super(toMessage(reason, message, code), cause);
        responseMessage = message;
        responseCode = code;
    }

    public HttpClientException(Throwable cause) {
        super(cause);
    }

    public String getResponseMessage() {
        return responseMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

}