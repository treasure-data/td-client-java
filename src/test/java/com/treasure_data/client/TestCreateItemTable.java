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
import com.treasure_data.model.CreateItemTableRequest;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.DataType;
import com.treasure_data.model.Database;

public class TestCreateItemTable extends
        PostMethodTestUtil<CreateItemTableRequest, CreateTableResult, DefaultClientAdaptorImpl> {

    private String databaseName;
    private String tableName;
    private String primaryKey;
    private DataType primaryKeyType;
    private CreateItemTableRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        primaryKey = "pk";
        primaryKeyType = DataType.INT;
        request = new CreateItemTableRequest(new Database(databaseName), tableName, primaryKey, primaryKeyType);
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
            CreateTableRequest request = new CreateItemTableRequest(database, "test01", "key", DataType.STRING);
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
        CreateTableResult result = doBusinessLogic();
        assertEquals(databaseName, result.getDatabase().getName());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("database", "testdb");
        map.put("table", "testtbl");
        map.put("type", "item");
        return JSONValue.toJSONString(map);
    }

    @Override
    public CreateTableResult doBusinessLogic() throws Exception {
        return clientAdaptor.createTable(request);
    }
}
