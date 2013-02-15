package com.treasure_data.client.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import com.treasure_data.client.Config;
import com.treasure_data.client.DefaultClientAdaptorImpl;
import com.treasure_data.client.GetMethodTestUtil;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeletePartRequest;
import com.treasure_data.model.bulkimport.DeletePartResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.ListPartsRequest;
import com.treasure_data.model.bulkimport.ListPartsResult;
import com.treasure_data.model.bulkimport.ListSessionsRequest;
import com.treasure_data.model.bulkimport.ListSessionsResult;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;

public class TestListParts
    extends GetMethodTestUtil<ListPartsRequest, ListPartsResult, BulkImportClientAdaptorImpl>{

    @Test @Ignore
    public void test00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataClient client = new TreasureDataClient(
                new TreasureDataCredentials(), System.getProperties());
        BulkImportClient biclient = new BulkImportClient(client);

        String sessionName = "t01";
        String databaseName = null;
        String tableName = null;
        Session sess = null;
        try {
            // create
//            {
//                CreateSessionRequest request = new CreateSessionRequest(sessionName, databaseName, tableName);
//                CreateSessionResult result = biclient.createSession(request);
//                sess = result.getSession();
//                System.out.println(sess);
//            }
            {
                sess = new Session(sessionName, databaseName, tableName);
                ListPartsRequest request = new ListPartsRequest(sess);
                ListPartsResult result = biclient.listParts(request);
                List<String> parts = result.getParts();
                System.out.println(parts);
            }
        } finally {
            // delete
//            DeleteSessionRequest request = new DeleteSessionRequest(sess);
//            DeleteSessionResult result = biclient.deleteSession(request);
//            System.out.println(result.getSessionName());
        }
    }

    private String sessionName;
    private String databaseName;
    private String tableName;
    private ListPartsRequest request;

    @Override
    public BulkImportClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        Properties props = System.getProperties();
        props.setProperty("td.api.key", "xxxx");
        TreasureDataClient client = new TreasureDataClient(props);
        return new BulkImportClientAdaptorImpl(client);
    }

    @Before
    public void createResources() throws Exception {
        super.createResources();
        sessionName = "testSess";
        databaseName = "testdb";
        tableName = "testtbl";
        request = new ListPartsRequest(new Session(sessionName, databaseName, tableName));
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        sessionName = null;
        databaseName = null;
        tableName = null;
        request = null;
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        ListPartsResult result = clientAdaptor.listParts(request);
        assertEquals(sessionName, result.getSession().getName());
        assertTrue(result.getParts() instanceof List);
    }

    @Override
    public String getJSONTextForChecking() {
        // {"name":"t01","parts":["error01","ok01"]}
        List<String> parts = new ArrayList<String>();
        parts.add("error01");
        parts.add("ok01");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", sessionName);
        map.put("parts", parts);
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.listParts(request);
    }
}
