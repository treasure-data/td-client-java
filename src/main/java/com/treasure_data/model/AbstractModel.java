package com.treasure_data.model;

abstract class AbstractModel implements Model {
    private String name;

    protected AbstractModel() {
        this(null);
    }

    protected AbstractModel(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
