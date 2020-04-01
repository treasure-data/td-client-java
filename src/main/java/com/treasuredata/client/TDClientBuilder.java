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

public class TDClientBuilder
        extends AbstractTDClientBuilder<TDClient, TDClientBuilder>
{
    /**
     * Create a new {@link TDClient} builder whose configuration is initialized with System Properties and $HOME/.td/td.conf values.
     * Precedence of properties is the following order:
     * <ol>
     * <li>System Properties</li>
     * <li>$HOME/.td/td.conf values</li>
     * </ol>
     *
     * @param loadTDConf
     */
    public TDClientBuilder(boolean loadTDConf)
    {
        super(loadTDConf);
    }

    @Override
    protected TDClientBuilder self()
    {
        return this;
    }

    @Override
    public TDClient build()
    {
        return new TDClient(buildConfig());
    }
}
