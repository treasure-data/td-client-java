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

import java.util.Random;

public class EqualJitterBackOff
        extends BackOff
{
    private final Random rnd = new Random();

    public EqualJitterBackOff()
    {
        this(2000, 60000, 2);
    }

    public EqualJitterBackOff(int baseIntervalMillis, int maxIntervalMillis, double multiplier)
    {
        super(baseIntervalMillis, maxIntervalMillis, multiplier);
    }

    @Override
    public int nextWaitTimeMillis()
    {
        executionCount++;
        int v = (int) calculateExponential() / 2;
        return v + rnd.nextInt(v);
    }
}
