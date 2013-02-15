package com.treasure_data.client.bulkimport;

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
import com.treasure_data.client.Config;
import com.treasure_data.client.PostMethodTestUtil;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.bulkimport.CommitSessionRequest;
import com.treasure_data.model.bulkimport.CommitSessionResult;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.Session;

public class TestCreateSession
        extends PostMethodTestUtil<CommitSessionRequest, CommitSessionResult, BulkImportClientAdaptorImpl> {

    @Test @Ignore
    public void test00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        TreasureDataClient client = new TreasureDataClient(
                new TreasureDataCredentials(), props);
        BulkImportClient biclient = new BulkImportClient(client);

        String sessionName = "sess01";
        String databaseName = "mugadb";
        String tableName = "test04";
        Session sess = null;
        try { // create
            CreateSessionRequest request = new CreateSessionRequest(sessionName, databaseName, tableName);
            CreateSessionResult result = biclient.createSession(request);
            sess = result.getSession();
            System.out.println(sess);
        } finally {
            // delete
            DeleteSessionRequest request = new DeleteSessionRequest(sess);
            DeleteSessionResult result = biclient.deleteSession(request);
            System.out.println(result.getSessionName());
        }

    }

    private String sessionName;
    private String databaseName;
    private String tableName;
    private CreateSessionRequest request;

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
        request = new CreateSessionRequest(sessionName, databaseName, tableName);
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
        CreateSessionResult result = clientAdaptor.createSession(request);
        assertEquals(sessionName, result.getSession().getName());
    }

    @Override
    public String getJSONTextForChecking() {
        // {"name":"sess01"}
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", sessionName);
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.createSession(request);
    }
}
