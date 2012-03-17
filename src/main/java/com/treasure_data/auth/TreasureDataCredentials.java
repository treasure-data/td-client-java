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
package com.treasure_data.auth;

import java.util.Properties;

import com.treasure_data.client.Config;

public class TreasureDataCredentials {
    private static String checkAPIKey(Properties props) {
        String apiKey = null;

        // check environment variable
        apiKey = System.getenv(Config.TD_ENV_API_KEY);
        if (apiKey != null) {
            return apiKey;
        }

        // check properties
        apiKey = props.getProperty(Config.TD_API_KEY);
        if (apiKey != null) {
            return apiKey;
        }

        // another setting...

        return apiKey; // null
    }

    private String apiKey;

    public TreasureDataCredentials() {
        this(System.getProperties());
    }
    
    public TreasureDataCredentials(Properties props) {
        this(checkAPIKey(props));
    }

    public TreasureDataCredentials(String apiKey) {
        this.apiKey = apiKey;
    }

    public void setAPIKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getAPIKey() {
	return apiKey;
    }

    @Override
    public String toString() {
	return String.format("%s{apiKey=%s}",
		TreasureDataCredentials.class.getName(),
		apiKey != null ? apiKey : "null");
    }
}
