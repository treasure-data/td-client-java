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

public interface Constants {
    String TD_ENV_API_KEY = "TREASURE_DATA_API_KEY";
    String TD_ENV_API_SERVER = "TD_API_SERVER";

    // There's no "https.proxyUser". These settings are used both for http and https.
    String HTTP_PROXY_USER = "http.proxyUser";
    String HTTP_PROXY_PASSWORD = "http.proxyPassword";

    String TD_API_KEY = "td.api.key";
    String TD_INTERNAL_KEY = "td.api.internalkey";
    String TD_INTERNAL_KEY_ID = "td.api.internalkeyid";

    String TD_CK_API_SERVER_SCHEME = "td.api.server.scheme";
    String TD_API_SERVER_SCHEME_HTTPS = "https://";
    String TD_API_SERVER_SCHEME_HTTP = "http://";
    String TD_API_SERVER_SCHEME_DEFAULTVALUE = TD_API_SERVER_SCHEME_HTTPS;

    String TD_API_SERVER_HOST = "td.api.server.host";
    String TD_API_SERVER_HOST_DEFAULTVALUE = "api.treasuredata.com";

    String TD_API_SERVER_PORT = "td.api.server.port";
    String TD_API_SERVER_PORT_HTTPS = "443";
    String TD_API_SERVER_PORT_HTTP = "80";
    String TD_API_SERVER_PORT_DEFAULTVALUE = TD_API_SERVER_PORT_HTTPS;

    String TD_AUTO_CREATE_TABLE = "td.create.table.auto";
    String TD_AUTO_CREATE_TABLE_DEFAULTVALUE = "false";

    String TD_CLIENT_CONNECT_TIMEOUT = "td.client.connect.timeout";
    String TD_CLIENT_CONNECT_TIMEOUT_DEFAULTVALUE = "" + 60 * 1000; // millis

    String TD_CLIENT_GETMETHOD_READ_TIMEOUT = "td.client.getmethod.read.timeout";
    String TD_CLIENT_GETMETHOD_READ_TIMEOUT_DEFAULTVALUE = "" + 600 * 1000; // millis

    String TD_CLIENT_PUTMETHOD_READ_TIMEOUT = "td.client.putmethod.read.timeout";
    String TD_CLIENT_PUTMETHOD_READ_TIMEOUT_DEFAULTVALUE = "" + 600 * 1000; // millis

    String TD_CLIENT_POSTMETHOD_READ_TIMEOUT = "td.client.postmethod.read.timeout";
    String TD_CLIENT_POSTMETHOD_READ_TIMEOUT_DEFAULTVALUE = "" + 600 * 1000; // millis

    String TD_CLIENT_RETRY_COUNT = "td.client.retry.count";
    String TD_CLIENT_RETRY_COUNT_DEFAULTVALUE = "8";

    String TD_CLIENT_RETRY_WAIT_TIME = "td.client.retry.wait.time";
    String TD_CLIENT_RETRY_WAIT_TIME_DEFAULTVALUE = "1000";
}
