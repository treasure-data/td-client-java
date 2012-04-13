//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
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

import com.treasure_data.auth.TreasureDataCredentials;

public class Config implements Constants {
    public static final String TD_ENV_API_KEY = "TREASURE_DATA_API_KEY";

    public static final String TD_ENV_API_SERVER = "TD_API_SERVER";

    public static final String TD_API_KEY = "td.api.key";

    public static final String TD_API_SERVER_HOST = "td.api.server.host";

    public static final String TD_API_SERVER_HOST_DEFAULT = "api.treasure-data.com";

    public static final String TD_API_SERVER_PORT = "td.api.server.port";

    public static final String TD_API_SERVER_PORT_DEFAULT = "80";

    public static final String TD_AUTO_CREATE_TABLE = "td.create.table.auto";

    public static final String TD_AUTO_CREATE_TABLE_DEFAULT = "false";

    private TreasureDataCredentials credentials;

    public void setCredentials(TreasureDataCredentials credentials) {
	this.credentials = credentials;
    }

    public TreasureDataCredentials getCredentials() {
	return credentials;
    }

}
