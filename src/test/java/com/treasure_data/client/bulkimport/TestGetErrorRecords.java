package com.treasure_data.client.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
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
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.ClientException;
import com.treasure_data.client.Config;
import com.treasure_data.client.GetMethodTestUtil;
import com.treasure_data.client.HttpClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.bulkimport.CommitSessionRequest;
import com.treasure_data.model.bulkimport.CommitSessionResult;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.FreezeSessionRequest;
import com.treasure_data.model.bulkimport.FreezeSessionResult;
import com.treasure_data.model.bulkimport.GetErrorRecordsRequest;
import com.treasure_data.model.bulkimport.GetErrorRecordsResult;
import com.treasure_data.model.bulkimport.PerformSessionRequest;
import com.treasure_data.model.bulkimport.PerformSessionResult;
import com.treasure_data.model.bulkimport.Session;
import com.treasure_data.model.bulkimport.UploadPartRequest;
import com.treasure_data.model.bulkimport.UploadPartResult;

public class TestGetErrorRecords
    extends GetMethodTestUtil<GetErrorRecordsRequest, GetErrorRecordsResult, BulkImportClientAdaptorImpl> {

    @Test @Ignore
    public void test01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));

        TreasureDataClient client = new TreasureDataClient(
                new TreasureDataCredentials(), System.getProperties());
        BulkImportClient biclient = new BulkImportClient(client);

        MessagePack msgpack = new MessagePack();
        long baseTime = System.currentTimeMillis() / 1000;
        Unpacker unpacker = biclient.getErrorRecords(new Session("mugasess", null, null));
        UnpackerIterator iter = unpacker.iterator();
        while (true) {
            boolean hasNext = iter.hasNext();
            System.out.println("hasNext: " + hasNext);
            if (!hasNext) {
                break;
            }
            System.out.println(iter.next());
        }
    }

    private String sessionName;
    private String databaseName;
    private String tableName;
    private GetErrorRecordsRequest request;

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
        request = new GetErrorRecordsRequest(new Session(sessionName, databaseName, tableName));
        responsedBinary = true;
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
    public Unpacker getMockResponseBodyBinary() {
        return new MessagePack().createBufferUnpacker();
    }

    @Override
    public void checkNormalBehavior0() throws Exception {
        GetErrorRecordsResult result = doBusinessLogic();
        assertEquals(sessionName, result.getSession().getName());
    }

    @Override
    public GetErrorRecordsResult doBusinessLogic() throws Exception {
        return clientAdaptor.getErrorRecords(request);
    }

    @Test @Ignore
    public void throwClientErrorWhenReceivedInvalidJSONAsResponseBody()
            throws Exception {
        super.throwClientErrorWhenReceivedInvalidJSONAsResponseBody();
    }

    @Test @Ignore
    public void throwClientErrorWhenGetResponseBodyThrowsIOError()
            throws Exception {
        super.throwClientErrorWhenGetResponseBodyThrowsIOError();
    }
}
