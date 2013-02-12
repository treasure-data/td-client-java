package com.treasure_data.client;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.model.CreateTableRequest;
import com.treasure_data.model.CreateTableResult;
import com.treasure_data.model.Database;
import com.treasure_data.model.ImportRequest;
import com.treasure_data.model.ImportResult;
import com.treasure_data.model.SwapTableRequest;
import com.treasure_data.model.SwapTableResult;
import com.treasure_data.model.Table;

public class TestSwapTable extends PostMethodTestUtil {

    private String databaseName;
    private String tableName1;
    private String tableName2;
    private SwapTableRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName1 = "origtbl";
        tableName2 = "newtbl";
        request = new SwapTableRequest(databaseName, tableName1, tableName2);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        tableName1 = null;
        tableName2 = null;
        request = null;
    }

    @Test @Ignore
    public void test00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials(props));
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "mugadb";
        String tableName1 = "test04";
        String tableName2 = "test04_1342157288";
        try {
            { // create
                CreateTableRequest request = new CreateTableRequest(
                        new Database(databaseName), tableName2);
                CreateTableResult result = clientAdaptor.createTable(request);
                Table table = result.getTable();
                System.out.println("create table: " + table.getName());
            }
            { // import
                Table table = new Table(new Database(databaseName), tableName2);
                byte[] data = createData();
                ImportRequest request = new ImportRequest(table, data);
                ImportResult result = clientAdaptor.importData(request);
                double dstTime = result.getElapsedTime();
                System.out.println("import table: " + table.getName());
                System.out.println("import time: " + dstTime);
            }
            { // swap
                SwapTableRequest request = new SwapTableRequest(databaseName, tableName1, tableName2);
                @SuppressWarnings("unused")
                SwapTableResult result = clientAdaptor.swapTable(request);
                System.out.println("swap table");
            }
        } finally {
            // delete
        }
    }

    private byte[] createData() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(out);
        MessagePack msgpack = new MessagePack();
        Packer packer = msgpack.createPacker(gzout);

        for (int i = 0; i < 300; i++) {
            long time = System.currentTimeMillis() / 1000;
            Map<String, Object> record = new HashMap<String, Object>();
            record.put("name", "muga:" + i);
            record.put("id", i);
            record.put("time", time);
            packer.write(record);
        }

        gzout.finish();
        return out.toByteArray();
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        SwapTableResult result = clientAdaptor.swapTable(request);
        assertEquals(databaseName, result.getDatabaseName());
        assertEquals(tableName1, result.getTableName1());
        assertEquals(tableName2, result.getTableName2());
    }

    @Override
    public String getJSONTextForChecking() {
        // {"database":"mugadb","table1":"test04","table2":"test04_1342157288"}
        Map map = new HashMap();
        map.put("database", databaseName);
        map.put("table1", tableName1);
        map.put("table2", tableName2);
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.swapTable(request);
    }
}
