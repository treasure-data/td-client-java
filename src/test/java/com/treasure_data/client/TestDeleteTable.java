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
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.DeleteTableResult;
import com.treasure_data.model.Table;

public class TestDeleteTable extends
        PostMethodTestUtil<DeleteTableRequest, DeleteTableResult> {

    private String databaseName;
    private String tableName;
    private DeleteTableRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        request = new DeleteTableRequest(new Table(
                new Database(databaseName), tableName, Table.Type.LOG));
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        request = null;
    }

    @Test @Ignore
    public void testDeleteTable00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "db1";
        try {
            // create database
            CreateDatabaseResult ret =
                clientAdaptor.createDatabase(new CreateDatabaseRequest(databaseName));
            Database database = ret.getDatabase();
            CreateTableRequest req = new CreateTableRequest(database, "test01");
            CreateTableResult res = clientAdaptor.createTable(req);
            Table table = res.getTable();

            DeleteTableRequest request = new DeleteTableRequest(table);
            DeleteTableResult result = clientAdaptor.deleteTable(request);
            System.out.println(result.getTableName());
        } finally {
            // delete database
            clientAdaptor.deleteDatabase(new DeleteDatabaseRequest(databaseName));
        }
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        DeleteTableResult result = clientAdaptor.deleteTable(request);
        assertEquals(databaseName, result.getDatabase().getName());
        assertEquals(tableName, result.getTableName());
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
        clientAdaptor.deleteTable(request);
    }
}
