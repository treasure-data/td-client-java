package com.treasure_data.client;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.model.ServerStatus;

public class TestTreasureDataClient {

    @Test @Ignore
    public void testGetServerStatus01() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        
        TreasureDataClient client = new TreasureDataClient(props);
        ServerStatus serverStatus = client.getServerStatus();
        System.out.println(serverStatus.getMessage());
    }
    
}
