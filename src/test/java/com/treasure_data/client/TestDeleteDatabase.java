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
import com.treasure_data.model.DeleteDatabaseResult;

public class TestDeleteDatabase extends PostMethodTestUtil {

    private String databaseName;
    private DeleteDatabaseRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        request = new DeleteDatabaseRequest(databaseName);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        request = null;
    }

    @Test @Ignore
    public void testDeleteDatabase00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "db1";
        {
            CreateDatabaseRequest request = new CreateDatabaseRequest(databaseName);
            CreateDatabaseResult result = clientAdaptor.createDatabase(request);
            System.out.println(result.getDatabase().getName());
        }
        {
            DeleteDatabaseRequest request = new DeleteDatabaseRequest(databaseName);
            DeleteDatabaseResult result = clientAdaptor.deleteDatabase(request);
            System.out.println(result.getDatabase().getName());
        }
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        DeleteDatabaseResult result = clientAdaptor.deleteDatabase(request);
        assertEquals(databaseName, result.getDatabaseName());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("database", "testdb");
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.deleteDatabase(request);
    }
}
