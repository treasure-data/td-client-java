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
import com.treasure_data.model.Database;
import com.treasure_data.model.ListTablesRequest;
import com.treasure_data.model.ListTablesResult;
import com.treasure_data.model.TableSummary;

public class TestListTables extends GetMethodTestUtil {

    private ListTablesRequest request;
    private String databaseName;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        request = new ListTablesRequest(new Database(databaseName));
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        request = null;
    }

    @Test @Ignore
    public void testListTables00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        //ListTablesRequest request = new ListTablesRequest(new Database("test_merge_0"));
        ListTablesRequest request = new ListTablesRequest(new Database("mugatest"));
        ListTablesResult result = clientAdaptor.listTables(request);
        List<TableSummary> tables = result.getTables();
        for (TableSummary table : tables) {
            System.out.println(table.getDatabase().getName());
            System.out.println(table.getName());
            System.out.println(table.getSchema());
            System.out.println(table.getType());
            System.out.println(table.getCount());
        }
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.listTables(request);
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        ListTablesResult result = clientAdaptor.listTables(request);
        List<TableSummary> tables = result.getTables();
        assertEquals(2, tables.size());
        assertEquals("foo", tables.get(0).getName());
        assertEquals("bar", tables.get(1).getName());
    }

    @Override
    public String getJSONTextForChecking() {
        Map map = new HashMap();
        map.put("database", "testdb");
        List tbls = new ArrayList();
        Map m0 = new HashMap();
        m0.put("type", "item");
        m0.put("name", "foo");
        m0.put("count", 13123233);
        tbls.add(m0);
        Map m1 = new HashMap();
        m1.put("type", "item");
        m1.put("name", "bar");
        m1.put("count", 331232);
        tbls.add(m1);
        map.put("tables", tbls);
        return JSONValue.toJSONString(map);
    }
}
