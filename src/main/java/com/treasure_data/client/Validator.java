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

import java.util.regex.Pattern;

import com.treasure_data.model.Request;

public class Validator {

    private static Pattern databaseNamePat = Pattern.compile("^([a-z0-9_]+)$");

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

    public void validateDatabaseName(String name) throws ClientException {
        if (name == null || name.isEmpty()) {
            throw new ClientException("Empty name is not allowed");
        }
        if (name.length() < 3 || 256 < name.length()) {
            throw new ClientException(String.format(
                    "Name must be 3 to 256 characters, got %d characters.",
                    name.length()));
        }
        if (!databaseNamePat.matcher(name).matches()) {
            throw new ClientException(
                    "Name must consist only of lower-case alphabets, numbers and '_'.");
        }
    }

    public void validateTableName(String name) throws ClientException {
        validateDatabaseName(name);
    }
}
