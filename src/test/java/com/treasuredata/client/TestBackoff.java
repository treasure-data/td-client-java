package com.treasuredata.client;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBackoff
{
    @Test(expected = IllegalArgumentException.class)
    public void invalidBaseIntervalArgument()
    {
        new ExponentialBackOff(-1, 10000, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidMaxIntervalArgument()
    {
        new ExponentialBackOff(1000, -1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidMultiplierArgument()
    {
        new FullJitterBackOff(1000, 10000, -1);
    }

    @Test
    public void nextWaitTimeIsUnderMaxIntervalValue()
    {
        int baseIntervalMillis = 1000;
        int maxIntervalMillis = 10000;
        double multiplier = 2.0;

        BackOff backOff = new FullJitterBackOff(baseIntervalMillis, maxIntervalMillis, multiplier);

        int exponential = backOff.nextWaitTimeMillis();
        assertTrue(exponential < maxIntervalMillis);
    }

    @Test
    public void nextWaitTimeIsUnderMaxIntervalValueWithEqualJitter()
    {
        int baseIntervalMillis = 1000;
        int maxIntervalMillis = 6000;
        double multiplier = 2.0;

        BackOff backOff = new EqualJitterBackOff(baseIntervalMillis, maxIntervalMillis, multiplier);

        int exponential = backOff.nextWaitTimeMillis();
        assertTrue(exponential < maxIntervalMillis);
    }

    @Test
    public void incrementCounter()
    {
        BackOff backOff = new FullJitterBackOff();
        int counter = 3;
        for (int i = 0; i < counter; i++) {
            backOff.incrementExecutionCount();
        }
        assertEquals(backOff.getExecutionCount(), counter);
    }
}
