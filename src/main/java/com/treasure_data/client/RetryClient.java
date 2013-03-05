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
package com.treasure_data.client;

import java.io.IOException;
import java.util.logging.Logger;

public class RetryClient {
    private static Logger LOG = Logger.getLogger(RetryClient.class.getName());

    public static interface Retryable {
        void doTry() throws ClientException;
    }

    public void retry(Retryable r, int retryCount) throws IOException {
        int count = 0;
        while (true) {
            try {
                r.doTry();
                if (count > 0) {
                    LOG.warning("Retry succeeded.");
                }
                break;
            } catch (ClientException e) {
                LOG.warning(e.getMessage());
                if (count >= retryCount) {
                    LOG.warning("Retry count exceeded limit.");
                    throw new IOException("Retry error");
                } else {
                    count++;
                    LOG.warning("It failed. but will be retried.");
                    waitRetry(1);
                }
            }
        }
    }

    public void retry(Retryable r, int retryCount, long waitSec) throws IOException {
        int count = 0;
        boolean notRetry = false;
        while (true) {
            try {
                r.doTry();
                break;
            } catch (ClientException e) {
                LOG.warning(e.getMessage());
                if (e instanceof HttpClientException
                        && ((HttpClientException) e).getResponseCode() < 400) {
                    count++;
                    waitRetry(waitSec);
                } else {
                    LOG.info("turned notRetry flag: " + notRetry);
                    notRetry = true;
                }
            } finally {
                if (count >= retryCount || notRetry) {
                    throw new IOException("Retry out error");
                }
            }
        }
    }

    protected void waitRetry(long sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
