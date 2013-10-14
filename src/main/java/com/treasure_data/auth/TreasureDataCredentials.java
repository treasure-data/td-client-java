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
package com.treasure_data.auth;

import java.util.Properties;

import com.treasure_data.client.Config;

public class TreasureDataCredentials {
    private void setProps(Properties props) {
        // check environment variable first for apikey
        this.apiKey = System.getenv(Config.TD_ENV_API_KEY);
        if (this.apiKey == null) {
            this.apiKey = props.getProperty(Config.TD_API_KEY);
        }

        // check properties
        this.internalKey = props.getProperty(Config.TD_INTERNAL_KEY);
        this.internalKeyId = props.getProperty(Config.TD_INTERNAL_KEY_ID);
    }

    private String apiKey;
    private String internalKey;
    private String internalKeyId;

    public TreasureDataCredentials() {
        this(System.getProperties());
    }

    public TreasureDataCredentials(Properties props) {
        setProps(props);
    }

    public TreasureDataCredentials(String apiKey) {
        this.apiKey = apiKey;
        this.internalKey = null;
        this.internalKeyId = null;
    }

    public void setAPIKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public String getAPIKey() {
        return apiKey;
    }

    public String getInternalKey() {
        return internalKey;
    }

    public String getInternalKeyId() {
        return internalKeyId;
    }

    @Override
    public String toString() {
        return String.format("%s{apiKey=%s}", getClass().getName(),
                apiKey != null ? apiKey : "null");
    }
}
