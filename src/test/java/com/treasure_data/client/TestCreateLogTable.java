package com.treasure_data.client;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.CreateLogTableRequest;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.LogTable;
import com.treasure_data.model.Table;

public class TestCreateLogTable extends TestCreateTableBase {

    @Override
    protected CreateTableRequest createRequest() {
        return new CreateLogTableRequest(new Database(databaseName), tableName);
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
    public void assertTableType(CreateTableResult result) {
        assertTrue(result.getTable() instanceof LogTable);
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("database", databaseName);
        map.put("table", tableName);
        map.put("type", Table.Type.LOG.type());
        return JSONValue.toJSONString(map);
    }
}
