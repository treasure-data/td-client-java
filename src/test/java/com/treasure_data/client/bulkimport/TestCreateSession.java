package com.treasure_data.client.bulkimport;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
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
import com.treasure_data.client.Config;
import com.treasure_data.client.HttpClientAdaptor;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.CreateDatabaseRequest;
import com.treasure_data.model.CreateDatabaseResult;
import com.treasure_data.model.DeleteDatabaseRequest;
import com.treasure_data.model.bulkimport.CreateSessionRequest;
import com.treasure_data.model.bulkimport.CreateSessionResult;
import com.treasure_data.model.bulkimport.DeleteSessionRequest;
import com.treasure_data.model.bulkimport.DeleteSessionResult;
import com.treasure_data.model.bulkimport.Session;

public class TestCreateSession {

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
}
