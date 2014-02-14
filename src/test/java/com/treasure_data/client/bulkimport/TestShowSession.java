package com.treasure_data.client.bulkimport;

import static org.junit.Assert.assertEquals;

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
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.SessionSummary;
import com.treasure_data.model.bulkimport.ShowSessionRequest;
import com.treasure_data.model.bulkimport.ShowSessionResult;

public class TestShowSession
    extends GetMethodTestUtil<ShowSessionRequest, ShowSessionResult, BulkImportClientAdaptorImpl>{

    @Test @Ignore
    public void test00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        TreasureDataClient client = new TreasureDataClient(
                new TreasureDataCredentials(), System.getProperties());
        BulkImportClient biclient = new BulkImportClient(client);

        ShowSessionRequest request = new ShowSessionRequest(new Session("mugasess", null, null));
        ShowSessionResult result = biclient.showSession(request);
        SessionSummary session = result.getSession();
        System.out.println(session);
    }

    private String sessionName;
    private String databaseName;
    private String tableName;
    private ShowSessionRequest request;

    @Override
    public BulkImportClientAdaptorImpl createClientAdaptorImpl(Config conf) {
        Properties props = conf.getProperties();
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
        request = new ShowSessionRequest(new Session(sessionName, databaseName, tableName));
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
        ShowSessionResult result = doBusinessLogic();
        SessionSummary session = result.getSession();
        assertEquals(sessionName, session.getName());
        assertEquals(tableName, session.getTableName());
        assertEquals(databaseName, session.getDatabaseName());
    }

    @Override
    public String getJSONTextForChecking() {
        //  {"name":"t01", "database":"sfdb", "table":"bi02",
        //      "status":"ready", "upload_frozen":false,
        //      "job_id":"70220", "valid_records":100,
        //      "error_records":10, "valid_parts":2, "error_parts":1}
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put("name", sessionName);
        ret.put("database", databaseName);
        ret.put("table", tableName);
        boolean afterPerforming = rand.nextBoolean();
        if (afterPerforming) {
            ret.put("upload_frozen", false);
            ret.put("job_id", "70220");
            ret.put("valid_records", 100);
            ret.put("error_records", 10);
            ret.put("valid_parts", 2);
            ret.put("error_parts", 1);
        }
        return JSONValue.toJSONString(ret);
    }

    @Override
    public ShowSessionResult doBusinessLogic() throws Exception {
        return clientAdaptor.showSession(request);
    }
}
