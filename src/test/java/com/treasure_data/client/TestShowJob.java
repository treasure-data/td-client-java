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
import com.treasure_data.model.ShowJobRequest;
import com.treasure_data.model.ShowJobResult;

public class TestShowJob extends
        GetMethodTestUtil<ShowJobRequest, ShowJobResult, DefaultClientAdaptorImpl> {

    private ShowJobRequest request;
    private String jobID;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        jobID = "12345";
        request = new ShowJobRequest(new Job(jobID));
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        jobID = null;
        request = null;
    }

    @Test @Ignore
    public void testShowJob00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        ShowJobRequest request = new ShowJobRequest(new Job("47555"));
        ShowJobResult result = clientAdaptor.showJob(request);
        System.out.println(result.getJobID());
        System.out.println(result.getJob().getStatus());
        System.out.println(result.getJob().getResultSchema());
    }

    @Override
    public ShowJobResult doBusinessLogic() throws Exception {
        return clientAdaptor.showJob(request);
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        ShowJobResult result = doBusinessLogic();
        assertEquals(jobID, result.getJob().getJobID());
        assertEquals(Job.Type.HIVE, result.getJob().getType());
        assertEquals(JobSummary.Status.SUCCESS, result.getJob().getStatus());
    }

    @Override
    public String getJSONTextForChecking() {
        Map map = new HashMap();
        map.put("type", "hive");
        map.put("query", "SELECT * FROM ACCESS");
        map.put("result", "output1");
        map.put("job_id", "12345");
        map.put("status", "success");
        map.put("status_str", "");
        map.put("url", "http://console.treasure-data.com/jobs/12345");
        map.put("created_at", "Sun Jun 26 17:39:18 -0400 2011");
        map.put("updated_at", "Sun Jun 26 17:39:54 -0400 2011");
        return JSONValue.toJSONString(map);
    }
}
