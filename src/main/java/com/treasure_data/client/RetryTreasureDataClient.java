package com.treasure_data.client;

import java.util.logging.Logger;

import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.ShowJobStatusRequest;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;

public class RetryTreasureDataClient {
    private static Logger LOG = Logger.getLogger(RetryTreasureDataClient.class.getName());

    protected TreasureDataClient client;

    public RetryTreasureDataClient(TreasureDataClient client) {
        this.client = client;
    }

    public JobSummary.Status showJobStatus(Job job, int retryCount, long waitSec)
            throws ClientException {
        int count = 0;
        JobSummary.Status stat;
        while (true) {
            try {
                stat = client.showJobStatus(new ShowJobStatusRequest(job)).getJobStatus();
                if (count > 0) {
                    LOG.warning("Retry succeeded.");
                }
                break;
            } catch (ClientException e) {
                LOG.warning(e.getMessage());
                if (count >= retryCount) {
                    LOG.warning("Retry count exceeded limit.");
                    throw new ClientException("Retry error");
                } else {
                    count++;
                    LOG.warning("It failed. but will be retried.");
                    waitRetry(waitSec);
                }
            }
        }
        return stat;
    }

    public SubmitJobResult submitJob(SubmitJobRequest request, int retryCount, long waitSec)
            throws ClientException {
        int count = 0;
        SubmitJobResult ret;
        while (true) {
            try {
                ret = client.submitJob(request);
                if (count > 0) {
                    LOG.warning("Retry succeeded.");
                }
                break;
            } catch (ClientException e) {
                LOG.warning(e.getMessage());
                if (count >= retryCount) {
                    LOG.warning("Retry count exceeded limit.");
                    throw new ClientException("Retry error");
                } else {
                    count++;
                    LOG.warning("It failed. but will be retried.");
                    waitRetry(waitSec);
                }
            }
        }
        return ret;
    }

    protected void waitRetry(long sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
