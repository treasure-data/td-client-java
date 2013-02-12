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
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.DeleteDatabaseRequest;

public class TestCreateDatabase extends PostMethodTestUtil {

    private String databaseName;
    private CreateDatabaseRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        request = new CreateDatabaseRequest(databaseName);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        request = null;
    }

    @Test @Ignore
    public void testCreateDatabase00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "db1";
        try {
            CreateDatabaseRequest request = new CreateDatabaseRequest(databaseName);
            CreateDatabaseResult result = clientAdaptor.createDatabase(request);
            System.out.println(result.getDatabase().getName());
        } finally {
            DeleteDatabaseRequest request = new DeleteDatabaseRequest(databaseName);
            clientAdaptor.deleteDatabase(request);
        }
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        CreateDatabaseResult result = clientAdaptor.createDatabase(request);
        assertEquals(databaseName, result.getDatabase().getName());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("database", databaseName);
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.createDatabase(request);
    }
}
