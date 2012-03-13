package com.treasure_data.model;

public class ServerStatusResult extends AbstractResult<Model> {

    private String message;

    public ServerStatusResult(String message) {
        super(null);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
