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
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;

public class TestCreateTable extends PostMethodTestUtil {

    private String databaseName;
    private String tableName;
    private CreateTableRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        request = new CreateTableRequest(new Database(databaseName), tableName);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        tableName = null;
        request = null;
    }

    @Test @Ignore
    public void testCreateTable00() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "mugadb";
        Database database = new Database(databaseName);
        try {
            CreateTableRequest request = new CreateTableRequest(database, "test01");
            CreateTableResult result = clientAdaptor.createTable(request);
            System.out.println(result.getTable().getName());
        } catch (ClientException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } finally {
            // delete database
            //clientAdaptor.deleteDatabase(new DeleteDatabaseRequest(databaseName));
        }
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        CreateTableResult result = clientAdaptor.createTable(request);
        assertEquals(databaseName, result.getDatabase().getName());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("database", "testdb");
        map.put("table", "testtbl");
        map.put("type", "log");
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.createTable(request);
    }
}
