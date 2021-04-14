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

/**
 *  An enum of Backoff strategies. In td-client-java, 3 backoff strategies are supported : {@link FullJitterBackOff}, {@link EqualJitterBackOff} & {@link ExponentialBackOff}.
 *  <p>
 *  {@link FullJitterBackOff} is recommend to use. The retry interval for it calculates with the following formula:</p>
 *  <pre>
 *      v = min(baseIntervalMillis * pow(multiplier, attempt), maxIntervalMillis);
 *      sleep = random_between(0, v)</pre>
 *  <p>
 *  The retry interval for {@link EqualJitterBackOff} is similar to {@link FullJitterBackOff}. The retry interval for it calculates with the following formula
 *  </p>
 *  <pre>
 *      v = min(baseIntervalMillis * pow(multiplier, attempt), maxIntervalMillis);
 *      sleep = v/2 + random_between(0, v/2)</pre>
 *  <p>
 *      {@link ExponentialBackOff} was used as default before. The retry interval for it extends linearly to the max interval.
 *  </p>
 */
public enum BackOffStrategy
{
    FullJitter,
    EqualJitter,
    Exponential;

    public static BackOff newBackOff(TDClientConfig config)
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
}
