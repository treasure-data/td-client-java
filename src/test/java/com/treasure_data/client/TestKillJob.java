package com.treasure_data.client;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.Job;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.KillJobRequest;
import com.treasure_data.model.KillJobResult;

public class TestKillJob extends
        PostMethodTestUtil<KillJobRequest, KillJobResult, DefaultClientAdaptorImpl> {

    private Job job;
    private KillJobRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        job = new Job("47555");
        request = new KillJobRequest(job);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        job = null;
        request = null;
    }

    @Test @Ignore
    public void testKillJob00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        KillJobRequest request = new KillJobRequest(new Job("47555"));
        KillJobResult result = clientAdaptor.killJob(request);
        System.out.println(result.getJobID());
    }

    @Test @Ignore
    public void testKillJob01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);

        KillJobRequest request = new KillJobRequest(new Job("5516862"));
        KillJobResult result = client.killJob(request);
        System.out.println(result.getJobID());
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        KillJobResult result = doBusinessLogic();
        assertEquals("12345", result.getJobID());
        assertEquals(JobSummary.Status.RUNNING, result.getFormerStatus());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("former_status", "running");
        map.put("job_id", "12345");
        return JSONValue.toJSONString(map);
    }

    @Override
    public KillJobResult doBusinessLogic() throws Exception {
        return clientAdaptor.killJob(request);
    }
}
