package com.treasure_data.client;

import com.treasure_data.model.Request;

public class Validator {

    public void checkCredentials(ClientAdaptor clientAdaptor, Request<?> request) throws ClientException {
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
