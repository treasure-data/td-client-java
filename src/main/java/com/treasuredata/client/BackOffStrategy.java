package com.treasuredata.client;

public enum BackOffStrategy
{
    FullJitter("fullJitter"),
    EqualJitter("equalJitter"),
    Exponential("exponential");

    private final String name;

    BackOffStrategy(String name)
    {
        this.name = name;
    }

    public String getText()
    {
        return name;
    }

    public static BackOffStrategy fromString(String strategyName)
    {
        for (BackOffStrategy strategy : BackOffStrategy.values()) {
            if (strategy.name.equals(strategyName)) {
                return strategy;
            }
        }
        return null;
    }
}
