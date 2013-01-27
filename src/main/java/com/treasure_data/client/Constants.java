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

    String TD_CLIENT_CONNECT_TIMEOUT = "td.client.connect.timeout";
    String TD_CLIENT_CONNECT_TIMEOUT_DEFAULTVALUE = "" + 60 * 1000; // millis

    String TD_CLIENT_GETMETHOD_READ_TIMEOUT = "td.client.getmethod.read.timeout";
    String TD_CLIENT_GETMETHOD_READ_TIMEOUT_DEFAULTVALUE = "" + 600 * 1000; // millis

    String TD_CLIENT_PUTMETHOD_READ_TIMEOUT = "td.client.putmethod.read.timeout";
    String TD_CLIENT_PUTMETHOD_READ_TIMEOUT_DEFAULTVALUE = "" + 600 * 1000; // millis

    String TD_CLIENT_POSTMETHOD_READ_TIMEOUT = "td.client.postmethod.read.timeout";
    String TD_CLIENT_POSTMETHOD_READ_TIMEOUT_DEFAULTVALUE = "" + 600 * 1000; // millis
}
