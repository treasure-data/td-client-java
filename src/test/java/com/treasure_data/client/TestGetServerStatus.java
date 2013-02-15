package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.ListDatabasesResult;
import com.treasure_data.model.Request;
import com.treasure_data.model.GetServerStatusRequest;
import com.treasure_data.model.GetServerStatusResult;

public class TestGetServerStatus extends
        GetMethodTestUtil<GetServerStatusRequest, GetServerStatusResult, DefaultClientAdaptorImpl> {

    private GetServerStatusRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        request = new GetServerStatusRequest(); 
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        request = null;
    }

    @Test @Ignore
    public void testGetServerStatus00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        GetServerStatusRequest request = new GetServerStatusRequest();
        GetServerStatusResult result = clientAdaptor.getServerStatus(request);
        System.out.println(result.getServerStatus().getMessage());
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.getServerStatus(request);
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        GetServerStatusResult result = clientAdaptor.getServerStatus(request);
        assertEquals("ok", result.getServerStatus().getMessage());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("status", "ok");
        return JSONValue.toJSONString(map);
    }
}
