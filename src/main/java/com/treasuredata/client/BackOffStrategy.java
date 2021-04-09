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

    public static BackOffStrategy fromString(String strategyName)
    {
        for (BackOffStrategy strategy : BackOffStrategy.values()) {
            if (strategy.name.equals(strategyName)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException("Illegal strategy name: " + strategyName);
    }

    public static BackOff newBackoff(TDClientConfig config)
    {
        BackOff backoff;
        switch (config.retryStrategy) {
            case Exponential:
                backoff = new ExponentialBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier);
                break;
            case EqualJitter:
                backoff = new EqualJitterBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier);
                break;
            case FullJitter:
            default:
                backoff = new FullJitterBackOff(config.retryInitialIntervalMillis, config.retryMaxIntervalMillis, config.retryMultiplier);
                break;
        }
        return backoff;
    }

    @Override
    public String toString()
    {
        return "BackOffStrategy{" +
                "name='" + name + '\'' +
                '}';
    }
}
