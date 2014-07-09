//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
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