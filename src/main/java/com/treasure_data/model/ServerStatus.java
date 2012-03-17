package com.treasure_data.model;

public class ServerStatus extends AbstractModel {

    public ServerStatus(String message) {
        super(message);
    }

    public String getMessage() {
        return getName();
    }
}
