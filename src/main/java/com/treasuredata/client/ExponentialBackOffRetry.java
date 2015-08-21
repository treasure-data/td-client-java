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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 *
 */
public class ExponentialBackOffRetry
{
    private final int initialWaitMills;
    private final int intervalWaitMillis;
    private final int maxRetryCount;
    private int executionCount;
    private int nextInterval;

    public ExponentialBackOffRetry(int maxRetryCount, int initialWaitMillis, int intervalWaitMillis)
    {
        this.initialWaitMills = initialWaitMillis;
        this.intervalWaitMillis = intervalWaitMillis;
        this.maxRetryCount = maxRetryCount;
        this.executionCount = 0;
        this.nextInterval = intervalWaitMillis;

        Preconditions.checkArgument(maxRetryCount >= 0, "maxRetryCount must be >= 0");
        Preconditions.checkArgument(initialWaitMillis >= 0, "initialWaitMillis must be >= 0");
        Preconditions.checkArgument(intervalWaitMillis >= 0, "intervalWaitMillios must be >= 0");
    }

    public int getExecutionCount()
    {
        return executionCount;
    }

    public int getMaxRetryCount()
    {
        return maxRetryCount;
    }

    public Optional<Integer> nextWaitTimeMillis()
    {
        if (executionCount >= maxRetryCount) {
            return Optional.absent();
        }
        else if (executionCount == 0) {
            executionCount++;
            return Optional.of(initialWaitMills);
        }
        else {
            executionCount++;
            int current = nextInterval;
            nextInterval *= 2;
            return Optional.of(current);
        }
    }
}
