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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.treasuredata.client.model.TDDatabase;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import com.treasuredata.client.model.TDTable;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 *
 */
public class Example
{
    protected Example()
    {
    }

    public static void main(String[] args)
    {
        TDClient client = TDClient.newClient();
        try {
            // Retrieve database and table names
            List<TDDatabase> databases = client.listDatabases();
            TDDatabase db = databases.get(0);
            System.out.println("database: " + db.getName());
            for (TDTable table : client.listTables(db.getName())) {
                System.out.println(" table: " + table);
            }

            // Submit a new Presto query
            String jobId = client.submit(TDJobRequest.newPrestoQuery("sample_datasets", "select count(1) cnt from www_access"));

            // Wait until the query finishes
            ExponentialBackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(jobId);
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(jobId);
            }

            // Read the detailed job information
            TDJob jobInfo = client.jobInfo(jobId);
            System.out.println("log:\n" + jobInfo.getCmdOut());
            System.out.println("error log:\n" + jobInfo.getStdErr());

            // Read the job results in msgpack.gz format
            client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Integer>() {
                @Override
                public Integer apply(InputStream input)
                {
                    int count = 0;
                    try {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
                        while (unpacker.hasNext()) {
                            // Each row of the query result is array type value (e.g., [1, "name", ...])
                            ArrayValue array = unpacker.unpackValue().asArrayValue();
                            System.out.println(array);
                            count++;
                        }
                        unpacker.close();
                    }
                    catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                    return count;
                }
            });
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }
}
