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
import com.treasure_data.model.AuthenticateRequest;
import com.treasure_data.model.AuthenticateResult;

public class TestAuthenticate extends
        PostMethodTestUtil<AuthenticateRequest, AuthenticateResult> {

    protected AuthenticateRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        String email = "muga";
        String password = "nishizawa";
        request = new AuthenticateRequest(email, password);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        request = null;
    }

    @Test @Ignore
    public void testAuthenticate00() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String email = props.getProperty("td.api.user");
        String password = props.getProperty("td.api.password");
        AuthenticateRequest request = new AuthenticateRequest(email, password);
        AuthenticateResult result = clientAdaptor.authenticate(request);
        System.out.println(result.getTreasureDataCredentials().getAPIKey());
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        AuthenticateResult result = clientAdaptor.authenticate(request);
        String gotApiKey = result.getTreasureDataCredentials().getAPIKey();
        assertEquals("xxxxapikey", gotApiKey);
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("user", "muga");
        map.put("apikey", "xxxxapikey");
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.authenticate(request);
    }
}
