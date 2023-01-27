/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
