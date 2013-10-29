package com.treasure_data.client;

import static org.junit.Assert.assertEquals;

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
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.SetTableSchemaRequest;
import com.treasure_data.model.SetTableSchemaResult;
import com.treasure_data.model.Table;
import com.treasure_data.model.TableSchema;

public class TestSetTableSchema extends
        PostMethodTestUtil<SetTableSchemaRequest, SetTableSchemaResult, DefaultClientAdaptorImpl> {

    private String databaseName;
    private String tableName;
    private List<String> pairs;
    private SetTableSchemaRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        TableSchema schema = new TableSchema(new Table(new Database(databaseName), tableName), pairs);
        request = new SetTableSchemaRequest(schema);
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
        String tableName = "sesstest";
        Database database = new Database(databaseName);
        try {
            TableSchema schema = new TableSchema(new Table(new Database(databaseName), tableName), pairs);
            SetTableSchemaRequest request = new SetTableSchemaRequest(schema);
            SetTableSchemaResult result = clientAdaptor.setTableSchema(request);
            System.out.println(result.getTableSchema().getName());
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
        SetTableSchemaResult result = doBusinessLogic();
        assertEquals(databaseName, result.getTableSchema().getDatabase().getName());
        assertEquals(tableName, result.getTableSchema().getName());
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
    public SetTableSchemaResult doBusinessLogic() throws Exception {
        return clientAdaptor.setTableSchema(request);
    }
}
