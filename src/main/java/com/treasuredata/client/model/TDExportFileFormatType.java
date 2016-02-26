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
package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

public enum TDExportFileFormatType
{
    TSV_GZ("tsv.gz"),
    JSONL_GZ("jsonl.gz"),
    JSON_GZ("json.gz"),
    JSON_LINE_GZ("json-line.gz");

    private String name;

    private TDExportFileFormatType(String name)
    {
        this.name = name;
    }

    @JsonCreator
    public static TDExportFileFormatType fromName(String name)
    {
        if ("tsv.gz".equals(name)) {
            return TSV_GZ;
        }
        else if ("jsonl.gz".equals(name)) {
            return JSONL_GZ;
        }
        else if ("json.gz".equals(name)) {
            return JSON_GZ;
        }
        else if ("json-line.gz".equals(name)) {
            return JSON_LINE_GZ;
        }
        throw new RuntimeJsonMappingException("Unexpected export file format type");
    }

    public String getTypeName()
    {
        return name;
    }

    @JsonValue
    @Override
    public String toString()
    {
        return name;
    }
}
