package com.treasure_data.auth;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.treasure_data.client.TestTreasureDataClient;

public class TestTreasureDataCredentials {

    @Test
    public void testConstructor01() throws IOException {
        Properties props = new Properties();
        props.load(TestTreasureDataClient.class.getClassLoader().getResourceAsStream("mock-treasure-data.properties"));
        TreasureDataCredentials credentials = new TreasureDataCredentials(props);
        assertTrue(credentials.getAPIKey() != null);
    }

}
