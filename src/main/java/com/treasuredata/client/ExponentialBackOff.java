/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.treasuredata.client;

import com.google.common.base.Preconditions;

/**
 *
 */
public class ExponentialBackOff
{
    private final int initialIntervalMills;
    private final int maxIntervalMillis;
    private final double multiplier;
    private int executionCount;
    private int nextIntervalMillis;

    public ExponentialBackOff()
    {
        this(2000, 60000, 1.5);
    }

    public ExponentialBackOff(int initialIntervalMillis, int maxIntervalMillis, double multiplier)
    {
        this.initialIntervalMills = initialIntervalMillis;
        this.maxIntervalMillis = maxIntervalMillis;
        this.multiplier = multiplier;
        this.executionCount = 0;
        this.nextIntervalMillis = initialIntervalMillis;

        Preconditions.checkArgument(initialIntervalMillis >= 0, "initialIntervalMillis must be >= 0");
        Preconditions.checkArgument(maxIntervalMillis >= 0, "maxIntervalMillis must be >= 0");
        Preconditions.checkArgument(multiplier >= 0.0, "multiplier must be >= 0");
    }

    public int getExecutionCount()
    {
        return executionCount;
    }

    public int nextWaitTimeMillis()
    {
        int currentWaitTimeMillis = nextIntervalMillis;
        nextIntervalMillis = Math.min((int) (nextIntervalMillis * multiplier), maxIntervalMillis);
        executionCount++;
        return currentWaitTimeMillis;
    }
}
