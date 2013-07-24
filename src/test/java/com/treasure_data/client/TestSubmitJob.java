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
import com.treasure_data.model.Database;
import com.treasure_data.model.Job;
import com.treasure_data.model.SubmitJobRequest;
import com.treasure_data.model.SubmitJobResult;
import com.treasure_data.model.Job.Priority;

public class TestSubmitJob extends
        PostMethodTestUtil<SubmitJobRequest, SubmitJobResult, DefaultClientAdaptorImpl> {

    private Job job;
    private SubmitJobRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        job = new Job(new Database("testdb"), "select * from testtbl", null);
        request = new SubmitJobRequest(job);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        job = null;
        request = null;
    }

    @Test @Ignore
    public void testSubmitJob00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        Database database = new Database("mugadb");
        String q = "select count(1) from score";
        Job job = new Job(database, q, Priority.HIGH, 1);
        SubmitJobRequest request = new SubmitJobRequest(job);
        SubmitJobResult result = clientAdaptor.submitJob(request);
        job = result.getJob();
        System.out.println(job.getJobID());
    }

    @Test @Ignore
    public void testSubmitJob01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        Database database = new Database("mugadb");
        String q = "select count(1) from www_access";
        Job job = new Job(database, Job.Type.IMPALA, q, null);
        SubmitJobRequest request = new SubmitJobRequest(job);
        SubmitJobResult result = clientAdaptor.submitJob(request);
        job = result.getJob();
        System.out.println(job.getJobID());
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        SubmitJobResult result = doBusinessLogic();
        Job job = result.getJob();
        assertEquals("12345", job.getJobID());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("job_id", "12345");
        map.put("database", "mugadb");
        return JSONValue.toJSONString(map);
    }

    @Override
    public SubmitJobResult doBusinessLogic() throws Exception {
        return clientAdaptor.submitJob(request);
    }
}
