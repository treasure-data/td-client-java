package com.treasure_data.client;

import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.model.ServerStatus;
import com.treasure_data.model.ServerStatusResult;

public class TestTreasureDataClient {

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(TestTreasureDataClient.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void testGetServerStatus01() throws Exception {
        Properties props = System.getProperties();
        
        TreasureDataClient client = new TreasureDataClient(props);
        ServerStatus serverStatus = client.getServerStatus();
        System.out.println(serverStatus.getMessage());
    }
    
}
