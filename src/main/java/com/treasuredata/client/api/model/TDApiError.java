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
package com.treasuredata.client.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class TDApiError
{
    private final String error;
    private final String text;
    private final String severity;

    @JsonCreator
    public TDApiError(
            @JsonProperty("error") String error,
            @JsonProperty("text") String text,
            @JsonProperty("severity") String severity)
    {
        this.error = error;
        this.text = text;
        this.severity = severity;
    }

    public String getError()
    {
        return error;
    }

    public String getText()
    {
        return text;
    }

    public String getSeverity()
    {
        return severity;
    }

    @Override
    public String toString() {
        return String.format("[%s] %s", severity, text);
    }

}

