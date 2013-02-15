package com.treasure_data.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import com.treasure_data.model.DeletePartialTableRequest;
import com.treasure_data.model.DeletePartialTableResult;
import com.treasure_data.model.ImportRequest;
import com.treasure_data.model.ImportResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.Table;

public class TestDeletePartialTable extends
        PostMethodTestUtil<DeletePartialTableRequest, DeletePartialTableResult, DefaultClientAdaptorImpl> {

    private String databaseName;
    private String tableName;
    private DeletePartialTableRequest request;

    @Override
    public DefaultClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        return new DefaultClientAdaptorImpl(conf);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        request = new DeletePartialTableRequest(new Table(
                new Database(databaseName), tableName, Table.Type.LOG), 3600 * 100, 3600 * 200);
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

        String database = "mugadb";
        String table = "test02";
        try {
            //createTable(clientAdaptor, database, table);
            //importData(clientAdaptor, database, table);
            deletePartialTable(clientAdaptor, database, table);
        } finally {
        }
    }

    private void createTable(DefaultClientAdaptorImpl clientAdaptor,
            String databaseName, String tableName) throws Exception {
        CreateTableRequest req = new CreateTableRequest(
                new Database(databaseName), tableName);
        CreateTableResult res = clientAdaptor.createTable(req);        
    }

    private void importData(DefaultClientAdaptorImpl clientAdaptor, String databaseName,
            String tableName) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(out);
        MessagePack msgpack = new MessagePack();
        Packer packer = msgpack.createPacker(gzout);

        long baseTime = 1337000400;//1340000000

        for (int i = 0; i < 500; i++) {
            long time = baseTime + 3600 * i;
            Map<String, Object> record = new HashMap<String, Object>();
            record.put("name", "muga:" + i);
            record.put("id", i);
            record.put("time", time);
            packer.write(record);
        }

        gzout.finish();
        byte[] bytes = out.toByteArray();

        ImportRequest request = new ImportRequest(
                new Table(new Database(databaseName), tableName), bytes);
        ImportResult result = clientAdaptor.importData(request);
    }

    private void deletePartialTable(DefaultClientAdaptorImpl clientAdaptor,
            String databaseName, String tableName) throws Exception {
        //long baseTime = 1337000400;//1340000000
        long from = 1337000400 + 3600 * 90;
        long to = 1337000400 + 3600 * 100;
        DeletePartialTableRequest req = new DeletePartialTableRequest(
                new Table(new Database(databaseName), tableName), from, to);
        DeletePartialTableResult res = clientAdaptor.deletePartialTable(req);
        System.out.println(res.getJobID());
    }

    @Test
    public void testValidateParameters() throws Exception {
        String databaseName = "testdb";
        String tableName = "testtbl";
        { // from = 3600 * 100, to = 3600 * 200
            new DeletePartialTableRequest(new Table(new Database(databaseName),
                    tableName, Table.Type.LOG), 3600 * 100, 3600 * 200);
        }
        { // from = 0, to = 0
            try {
                new DeletePartialTableRequest(new Table(new Database(databaseName),
                        tableName, Table.Type.LOG), 0, 0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof IllegalArgumentException);
            }
        }
        { // from = -1, to = 3600 * 200
            try {
                new DeletePartialTableRequest(new Table(new Database(databaseName),
                        tableName, Table.Type.LOG), -1, 3600 * 200);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof IllegalArgumentException);
            }
        }
        { // from = 3600 * 100, to = -1
            try {
                new DeletePartialTableRequest(new Table(new Database(databaseName),
                        tableName, Table.Type.LOG), 3600 * 100, -1);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof IllegalArgumentException);
            }
        }
        { // from = 3600 * 300, to = 3600 * 200
            try {
                new DeletePartialTableRequest(new Table(new Database(databaseName),
                        tableName, Table.Type.LOG), 3600 * 300, 3600 * 200);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof IllegalArgumentException);
            }
        }
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        DeletePartialTableResult result = doBusinessLogic();
        Job job = result.getJob();
        assertEquals(databaseName, job.getDatabase().getName());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("job_id", "12345");
        map.put("database", "testdb");
        return JSONValue.toJSONString(map);
    }

    @Override
    public DeletePartialTableResult doBusinessLogic() throws Exception {
        return clientAdaptor.deletePartialTable(request);
    }
}
