//
// Java Client Library for Treasure Data Cloud
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
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

import java.io.IOException;
import java.io.InputStream;

import com.treasure_data.client.ClientException;

/**
 * this class is developed for td-jdbc
 */
public class JobResult2 extends JobResult {

    private int experimentalCode = 0;
    private InputStream in;

    public JobResult2(Job job) {
        this(job, 0);
    }

    public JobResult2(Job job, int experimentCode) {
        super(job);
        this.experimentalCode = experimentCode;
    }

    public int getExperimentalCode() {
        return experimentalCode;
    }

    public void close() throws ClientException {
        if (getResult() != null) {
            try {
                getResult().close();
            } catch (IOException e) {
                throw new ClientException(e);
            }
        }
    }

    public void setResultInputStream(InputStream in) {
        this.in = in;
    }

    public InputStream getResultInputStream() {
        return in;
    }
}
