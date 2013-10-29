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
import com.treasure_data.model.ShowJobStatusRequest;
import com.treasure_data.model.ShowJobStatusResult;

public class TestShowJobStatus extends
        GetMethodTestUtil<ShowJobStatusRequest, ShowJobStatusResult, DefaultClientAdaptorImpl> {

    private ShowJobStatusRequest request;
    private String jobID;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        jobID = "12345";
        request = new ShowJobStatusRequest(new Job(jobID));
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        jobID = null;
        request = null;
    }

    @Test @Ignore
    public void testShowJobStatus00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        ShowJobStatusRequest request = new ShowJobStatusRequest(new Job("4702432"));
        ShowJobStatusResult result = clientAdaptor.showJobStatus(request);
        System.out.println(result.getJobStatus());
    }

    @Test @Ignore
    public void testShowJobStatus01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(props);

        ShowJobStatusRequest request = new ShowJobStatusRequest(new Job("4702432"));
        ShowJobStatusResult result = client.showJobStatus(request);
        System.out.println(result.getJobStatus());
    }

    @Override
    public ShowJobStatusResult doBusinessLogic() throws Exception {
        return clientAdaptor.showJobStatus(request);
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        ShowJobStatusResult result = doBusinessLogic();
        assertEquals(JobSummary.Status.SUCCESS, result.getJobStatus());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public String getJSONTextForChecking() {
        Map map = new HashMap();
        map.put("job_id", "12345");
        map.put("status", "success");
        map.put("start_at", "Sun Jun 26 17:39:18 -0400 2011");
        map.put("created_at", "Sun Jun 26 17:39:18 -0400 2011");
        map.put("updated_at", "Sun Jun 26 17:39:54 -0400 2011");
        map.put("end_at", "Sun Jun 26 17:39:18 -0400 2011");
        Map debugMap = new HashMap();
        debugMap.put("cmdout", "commandoutput");
        debugMap.put("stderr", "standarderror");
        map.put("debug", debugMap);
        return JSONValue.toJSONString(map);
    }
}
