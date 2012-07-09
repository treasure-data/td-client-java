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

import com.treasure_data.model.Request;

public class Validator {

    public void validateCredentials(TreasureDataClient client, Request<?> request)
            throws ClientException {
        String apiKey = request.getCredentials().getAPIKey();
        if (apiKey != null) {
            return;
        }

        apiKey = client.getTreasureDataCredentials().getAPIKey();
        if (apiKey != null) {
            request.setCredentials(client.getTreasureDataCredentials());
        }

        throw new ClientException("api key is not set.");
    }

    public void validateCredentials(DefaultClientAdaptor clientAdaptor, Request<?> request)
            throws ClientException {
        String apiKey = request.getCredentials().getAPIKey();
        if (apiKey != null) {
            return;
        }

        apiKey = clientAdaptor.getTreasureDataCredentials().getAPIKey();
        if (apiKey != null) {
            request.setCredentials(clientAdaptor.getTreasureDataCredentials());
        }

        throw new ClientException("api key is not set.");
    }

    public void validateJSONData(String jsonData) throws ClientException {
        if (jsonData == null) {
            throw new ClientException(
                    "JSON data that was returned by server is null");
        }
    }

    public void validateJavaObject(String jsonData, Object obj) throws ClientException {
        if (obj == null) {
            throw new ClientException(String.format(
                    "Server error (invalid JSON Data): %s", jsonData));
        }
    }
}
