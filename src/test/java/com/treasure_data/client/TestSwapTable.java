package com.treasure_data.client;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

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

public class TestSwapTable {

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void test00() throws Exception {
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl tdhclient = new DefaultClientAdaptorImpl(conf);

        String databaseName = "mugadb";
        String tableName1 = "test03";
        String tableName2 = "test03_1342157288";
        try {
            { // create
                CreateTableRequest request = new CreateTableRequest(
                        new Database(databaseName), tableName2);
                CreateTableResult result = tdhclient.createTable(request);
                Table table = result.getTable();
                System.out.println("create table: " + table.getName());
            }
            { // import
                Table table = new Table(new Database(databaseName), tableName2);
                byte[] data = createData();
                ImportRequest request = new ImportRequest(table, data);
                ImportResult result = tdhclient.importData(request);
                double dstTime = result.getElapsedTime();
                System.out.println("import table: " + table.getName());
                System.out.println("import time: " + dstTime);
            }
            { // swap
                SwapTableRequest request = new SwapTableRequest(databaseName, tableName1, tableName2);
                SwapTableResult result = tdhclient.swapTable(request);
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
}
