package com.treasuredata.client;

public interface BackOff
{
    int getExecutionCount();

    void incrementExecutionCount();

    int nextWaitTimeMillis();
}
