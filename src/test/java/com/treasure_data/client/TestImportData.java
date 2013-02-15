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
import com.treasure_data.model.DeleteTableRequest;
import com.treasure_data.model.ImportRequest;
import com.treasure_data.model.ImportResult;
import com.treasure_data.model.Table;

public class TestImportData extends
        PutMethodTestUtil<ImportRequest, ImportResult, DefaultClientAdaptorImpl> {

    private ImportRequest request;
    private String databaseName;
    private String tableName;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        byte[] bytes = new byte[32];
        request = new ImportRequest(new Table(new Database(databaseName), tableName), bytes);
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        tableName = null;
        request = null;
    }

    @Test @Ignore
    public void testImportData00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String databaseName = "mugadb";
        String tableName = "testtbl";
        Table dstTable = null;
        try {
            CreateTableRequest preReq = new CreateTableRequest(
                    new Database(databaseName), tableName);
            CreateTableResult preRes = clientAdaptor.createTable(preReq);
            Table srcTable = preRes.getTable();

            byte[] data = createData();
            ImportRequest request = new ImportRequest(srcTable, data);
            ImportResult result = clientAdaptor.importData(request);
            dstTable = result.getTable();
            double dstTime = result.getElapsedTime();
            System.out.println("dst table: " + dstTable);
            System.out.println("dst time: " + dstTime);
        } finally {
            if (dstTable != null) {
                // delete table
                clientAdaptor.deleteTable(new DeleteTableRequest(dstTable));
            }
        }
    }

    private byte[] createData() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(out);
        MessagePack msgpack = new MessagePack();
        Packer packer = msgpack.createPacker(gzout);

        for (int i = 0; i < 100; i++) {
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
    public void doBusinessLogic() throws Exception {
        clientAdaptor.importData(request);
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        ImportResult result = clientAdaptor.importData(request);
        assertEquals(databaseName, result.getTable().getDatabase().getName());
        assertEquals(tableName, result.getTable().getName());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("database", databaseName);
        map.put("table", tableName);
        map.put("elapsed_time", 32.3);
        return JSONValue.toJSONString(map);
    }
}
