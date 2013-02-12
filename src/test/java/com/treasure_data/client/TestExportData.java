package com.treasure_data.client;

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
import com.treasure_data.model.Database;
import com.treasure_data.model.ExportRequest;
import com.treasure_data.model.ExportResult;
import com.treasure_data.model.Job;
import com.treasure_data.model.Table;

public class TestExportData extends PostMethodTestUtil {

    private String databaseName;
    private String tableName;
    private ExportRequest request;

    @Before
    public void createResources() throws Exception {
        super.createResources();
        databaseName = "testdb";
        tableName = "testtbl";
        request = new ExportRequest(new Table(new Database(databaseName),
                tableName), "s3", "bucket1", "json.gz", "xxx", "yyy");
    }

    @After
    public void deleteResources() throws Exception {
        super.deleteResources();
        databaseName = null;
        tableName = null;
        request = null;
    }

    @Test @Ignore
    public void testExportData00() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        Config conf = new Config();
        conf.setCredentials(new TreasureDataCredentials());
        DefaultClientAdaptorImpl clientAdaptor = new DefaultClientAdaptorImpl(conf);

        String bucketName = props.getProperty("td.client.export.bucket");
        String accessKeyID = props.getProperty("td.client.accesskey.id");
        String secretAccessKey = props.getProperty("td.client.secret.accesskey");
        Database database = new Database("mugadb");
        Table table = new Table(database, "mugatbl");
        ExportRequest request = new ExportRequest(
                table, "s3", bucketName, "json.gz", accessKeyID, secretAccessKey);
        ExportResult result = clientAdaptor.exportData(request);
        Job job = result.getJob();
        System.out.println(job.getJobID());
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        ExportResult result = clientAdaptor.exportData(request);
        Job job = result.getJob();
        assertEquals(databaseName, job.getDatabase().getName());
        assertEquals("12345", job.getJobID());
    }

    @Override
    public String getJSONTextForChecking() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("job_id", "12345");
        map.put("database", databaseName);
        return JSONValue.toJSONString(map);
    }

    @Override
    public void doBusinessLogic() throws Exception {
        clientAdaptor.exportData(request);
    }
}
