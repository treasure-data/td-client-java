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

abstract class AbstractBackOff
        implements BackOff
{
    protected final int baseIntervalMillis;
    protected final int maxIntervalMillis;
    protected final double multiplier;
    protected int executionCount;

    protected AbstractBackOff(int baseIntervalMillis, int maxIntervalMillis, double multiplier)
    {
        this.baseIntervalMillis = baseIntervalMillis;
        this.maxIntervalMillis = maxIntervalMillis;
        this.multiplier = multiplier;
        this.executionCount = 0;

        if (baseIntervalMillis < 0) {
            throw new IllegalArgumentException("baseIntervalMillis must be >= 0");
        }
        if (maxIntervalMillis < 0) {
            throw new IllegalArgumentException("maxIntervalMillis must be >= 0");
        }
        if (multiplier < 0.0) {
            throw new IllegalArgumentException("multiplier must be >= 0");
        }
    }

    protected double calculateExponential()
    {
        return Math.min(baseIntervalMillis * Math.pow(multiplier, executionCount), maxIntervalMillis);
    }

    @Override
    public int getExecutionCount()
    {
        return executionCount;
    }

    @Override
    public void incrementExecutionCount()
    {
        executionCount++;
    }
}
