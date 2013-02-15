package com.treasure_data.client.bulkimport;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.Config;
import com.treasure_data.client.GetMethodTestUtil;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.ListSessionsRequest;
import com.treasure_data.model.bulkimport.ListSessionsResult;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;

public class TestListSessions
    extends GetMethodTestUtil<ListSessionsRequest, ListSessionsResult, BulkImportClientAdaptorImpl>{

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

        String sessionName = "sess01";
        String databaseName = "mugadb";
        String tableName = "test04";
        Session sess = null;
        try {
            // create
            {
                CreateSessionRequest request = new CreateSessionRequest(sessionName, databaseName, tableName);
                CreateSessionResult result = biclient.createSession(request);
                sess = result.getSession();
                System.out.println(sess);
            }
            {
                ListSessionsRequest request = new ListSessionsRequest();
                ListSessionsResult result = biclient.listSessions(request);
                List<SessionSummary> sessions = result.getSessions();
                for (SessionSummary session : sessions) {
                    System.out.println(session);
                }
            }
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
    private ListSessionsRequest request;

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
        request = new ListSessionsRequest();
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
        ListSessionsResult result = clientAdaptor.listSessions(request);
        assertTrue(result.getSessions() instanceof List);
    }

    @Override
    public String getJSONTextForChecking() {
        // {"bulk_imports":
        //  [{"name":"t01", "database":"sfdb", "table":"bi02",
        //      "status":"ready", "upload_frozen":false,
        //      "job_id":"70220", "valid_records":100,
        //      "error_records":10, "valid_parts":2, "error_parts":1},
        //   {"name":"sess01", "database":"mugadb", "table":"test04",
        //      "status":"uploading", "upload_frozen":false,
        //      "job_id":null, "valid_records":null,
        //      "error_records":null, "valid_parts":null, "error_parts":null}]}
        List<Map<String, Object>> sesses = new ArrayList<Map<String, Object>>();
        Map<String, Object> sess01 = new HashMap<String, Object>();
        sess01.put("name", sessionName);
        sess01.put("database", databaseName);
        sess01.put("table", tableName);
        sess01.put("upload_frozen", false);
        sess01.put("job_id", "70220");
        sess01.put("valid_records", 100);
        sess01.put("error_records", 10);
        sess01.put("valid_parts", 2);
        sess01.put("error_parts", 1);
        Map<String, Object> sess02 = new HashMap<String, Object>();
        sess02.put("name", "sess02");
        sess02.put("database", "mugadb");
        sess02.put("table", "mugatbl");
        sess02.put("upload_frozen", false);
        sess02.put("job_id", "70221");
        sess02.put("valid_records", null);
        sess02.put("error_records", null);
        sess02.put("valid_parts", null);
        sess02.put("error_parts", null);
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put("bulk_imports", sesses);
        return JSONValue.toJSONString(ret);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.listSessions(request);
    }
}
