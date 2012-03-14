//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.model;

public class JobResult extends AbstractModel {

    public static enum Format {
        MSGPACK, JSON, UNKNOWN,
    }

    public static Format toFormat(String formatName) {
        if (formatName.equals("msgpack")) {
            return Format.MSGPACK;
        } else if (formatName.equals("json")) {
            return Format.JSON;
        } else {
            return Format.UNKNOWN;
        }
    }

    public static String toFormatName(Format format) {
        switch (format) {
        case MSGPACK:
            return "msgpack";
        case JSON:
            return "json";
        default:
            return "unknown";
        }
    }

    private Job job;

    private org.msgpack.type.Value result;

    private Format format;

    public JobResult(String jobID) {
        this(new Job(jobID), Format.MSGPACK);
    }

    public JobResult(Job job, Format format) {
        this(job, format, null);
    }

    private JobResult(Job job, Format format, org.msgpack.type.Value result) {
        super(job.getJobID());
        this.job = job;
        this.format = format;
        this.result = result;
    }

    public Job getJob() {
        return job;
    }

    public void setResult(org.msgpack.type.Value result) {
        this.result = result;
    }

    public org.msgpack.type.Value getResult() {
        return result;
    }

    public Format getFormat() {
        return format;
    }
}
