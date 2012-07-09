package com.treasure_data.client;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.GetJobResultRequest;
import com.treasure_data.model.GetJobResultResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobResult;

public class TestHttpClientAdaptor {
    @Test
    public void testSetTreasureDataCredentials() throws Exception {
        Config conf = new Config();
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        {
            TreasureDataCredentials credentails = clientAdaptor.getTreasureDataCredentials();
            assertTrue(credentails == null);
        }

        {
            Properties props = new Properties();
            props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
            clientAdaptor.setTreasureDataCredentials(new TreasureDataCredentials(props));
            TreasureDataCredentials credentails = clientAdaptor.getTreasureDataCredentials();
            assertTrue(credentails != null);
            assertTrue(credentails.getAPIKey() != null);
        }
    }

    // TODO

    @Test @Ignore
    public void testGetJobResult01() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);
        //clientAdaptor.setConnection(new MockHttpConnectionImpl0());

        GetJobResultRequest request = new GetJobResultRequest(
                new JobResult(new Job("26317")));
        GetJobResultResult result = clientAdaptor.getJobResult(request);
        System.out.println(result.getJob().getJobID());
    }
}
