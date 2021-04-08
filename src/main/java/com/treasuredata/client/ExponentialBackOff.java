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

/**
 *
 */
public class ExponentialBackOff
        extends AbstractBackOff
{
    private int nextIntervalMillis;

    public ExponentialBackOff()
    {
        this(2000, 60000, 1.5);
    }

    public ExponentialBackOff(int initialIntervalMillis, int maxIntervalMillis, double multiplier)
    {
        super(initialIntervalMillis, maxIntervalMillis, multiplier);
        this.nextIntervalMillis = initialIntervalMillis;
    }

    @Override
    public int nextWaitTimeMillis()
    {
        int currentWaitTimeMillis = nextIntervalMillis;
        nextIntervalMillis = Math.min((int) (nextIntervalMillis * multiplier), maxIntervalMillis);
        executionCount++;
        return currentWaitTimeMillis;
    }
}
