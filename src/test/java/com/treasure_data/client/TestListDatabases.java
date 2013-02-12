package com.treasure_data.client;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
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
import com.treasure_data.model.ListDatabasesRequest;
import com.treasure_data.model.ListDatabasesResult;

public class TestListDatabases extends GetMethodTestUtil {

    private ListDatabasesRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        request = new ListDatabasesRequest();
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        request = null;
    }

    @Test @Ignore
    public void testListDatabases00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        ListDatabasesRequest request = new ListDatabasesRequest();
        ListDatabasesResult result = clientAdaptor.listDatabases(request);
        List<DatabaseSummary> databases = result.getDatabases();
        for (DatabaseSummary database : databases) {
            System.out.println(database.getName());
        }
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.listDatabases(request);
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        ListDatabasesResult result = clientAdaptor.listDatabases(request);
        List<DatabaseSummary> databases = result.getDatabases();
        assertEquals(2, databases.size());
        assertEquals("foo", databases.get(0).getName());
        assertEquals("bar", databases.get(1).getName());
    }

    @Override
    public String getJSONTextForChecking() {
        List ary = new ArrayList();
        Map m0 = new HashMap();
        m0.put("name", "foo");
        m0.put("count", 10);
        m0.put("created_at", "created_time");
        m0.put("updated_at", "updated_time");
        ary.add(m0);
        Map m1 = new HashMap();
        m1.put("name", "bar");
        m1.put("count", 10);
        m1.put("created_at", "created_time");
        m1.put("updated_at", "updated_time");
        ary.add(m1);
        Map map = new HashMap();
        map.put("databases", ary);
        return JSONValue.toJSONString(map);
    }
}
