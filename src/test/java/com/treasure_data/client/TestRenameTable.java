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
import com.treasure_data.model.RenameTableRequest;
import com.treasure_data.model.RenameTableResult;
import com.treasure_data.model.SwapTableRequest;
import com.treasure_data.model.SwapTableResult;
import com.treasure_data.model.Table;

public class TestRenameTable extends
        PostMethodTestUtil<RenameTableRequest, RenameTableResult, DefaultClientAdaptorImpl> {

    private String databaseName;
    private String origTableName;
    private String newTableName;
    private RenameTableRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        origTableName = "origtbl";
        newTableName = "newtbl";
        request = new RenameTableRequest(databaseName, origTableName, newTableName);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        origTableName = null;
        newTableName = null;
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
        String origTableName = "test02";
        String newTableName = "test03";
        try {
            { // create
                CreateTableRequest request = new CreateTableRequest(
                        new Database(databaseName), origTableName);
                CreateTableResult result = clientAdaptor.createTable(request);
                Table table = result.getTable();
                System.out.println("create table: " + table.getName());
            }
            { // rename
                RenameTableRequest request = new RenameTableRequest(databaseName, origTableName, newTableName);
                @SuppressWarnings("unused")
                RenameTableResult result = clientAdaptor.renameTable(request);
                System.out.println("rename table");
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
        RenameTableResult result = doBusinessLogic();
        assertEquals(databaseName, result.getDatabaseName());
        assertEquals(origTableName, result.getOrigTableName());
        assertEquals(newTableName, result.getNewTableName());
    }

    @Override
    public String getJSONTextForChecking() {
        // {"database":"mugadb","table1":"test04","table2":"test04_1342157288"}
        Map map = new HashMap();
        map.put("database", databaseName);
        map.put("table", origTableName);
        map.put("type", "log");
        return JSONValue.toJSONString(map);
    }

    @Override
    public RenameTableResult doBusinessLogic() throws Exception {
        return clientAdaptor.renameTable(request);
    }
}
