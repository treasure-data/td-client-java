package com.treasure_data.client.bulkimport;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.FreezeSessionRequest;
import com.treasure_data.model.bulkimport.FreezeSessionResult;
import com.treasure_data.model.bulkimport.PerformSessionRequest;
import com.treasure_data.model.bulkimport.PerformSessionResult;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.UploadPartRequest;
import com.treasure_data.model.bulkimport.UploadPartResult;

public class TestPerformSession {

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void test00() throws Exception {
        TreasureDataClient client = new TreasureDataClient(
                new TreasureDataCredentials(), System.getProperties());
        BulkImportClient biclient = new BulkImportClient(client);

        MessagePack msgpack = new MessagePack();
        long baseTime = System.currentTimeMillis() / 1000;


        String sessionName = "sess01";
        String databaseName = "mugadb";
        String tableName = "test04";
        Session sess = null;
        try { // create
            {
                CreateSessionRequest request = new CreateSessionRequest(sessionName, databaseName, tableName);
                CreateSessionResult result = biclient.createSession(request);
                sess = result.getSession();
                System.out.println(sess);
            }

            { // upload
                List<String> parts = new ArrayList<String>();
                parts.add("01d");
                parts.add("02d");
                parts.add("03d");

                for (int i = 0; i < parts.size(); i++) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    GZIPOutputStream gzout = new GZIPOutputStream(out);
                    Packer pk = msgpack.createPacker(gzout);
                    Map<String, Object> src = new HashMap<String, Object>();
                    src.put("k", i);
                    src.put("v", "muga:" + i);
                    src.put("time", baseTime + 3600 * i);
                    pk.write(src);
                    gzout.finish();
                    gzout.close();
                    byte[] bytes = out.toByteArray();

                    UploadPartRequest request = new UploadPartRequest(sess, parts.get(i), bytes);
                    UploadPartResult result = biclient.uploadPart(request);
                }
            }

            { // freeze
                FreezeSessionRequest request = new FreezeSessionRequest(sess);
                FreezeSessionResult result = biclient.freezeSession(request);
            }

            { // perform
                PerformSessionRequest request = new PerformSessionRequest(sess);
                PerformSessionResult result = biclient.performSession(request);
            }
        } finally {
            // delete
//            DeleteSessionRequest request = new DeleteSessionRequest(sess);
//            DeleteSessionResult result = biclient.deleteSession(request);
//            System.out.println(result.getSessionName());
        }

    }
}
