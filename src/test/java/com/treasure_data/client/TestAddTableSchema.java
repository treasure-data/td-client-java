package com.treasure_data.client;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.swing.event.ListSelectionEvent;

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

public class TestAddTableSchema {

    private String databaseName;
    private String tableName;
    private List<String> pairs;
    private SetTableSchemaRequest request;

    @Test @Ignore
    public void testAddTableSchema00() throws Exception {
        Properties props = new Properties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "mugadb";
        String tableName = "foo";
        Database database = new Database(databaseName);
        try {
            clientAdaptor.addTableSchema(databaseName, tableName, Arrays.asList("b:string", "c:int"));
            //clientAdaptor.removeTableSchema(databaseName, tableName, Arrays.asList("b", "c"));
        } catch (ClientException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } finally {
            // delete database
            //clientAdaptor.deleteDatabase(new DeleteDatabaseRequest(databaseName));
        }
    }


}
