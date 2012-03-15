package com.treasure_data.auth;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.treasure_data.client.TestTreasureDataClient;

public class TestTreasureDataCredentials {

    @Test
    public void testConstructor01() throws IOException {
        Properties props = new Properties();
        props.load(TestTreasureDataClient.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
        TreasureDataCredentials credentials = new TreasureDataCredentials(props);
        assertTrue(credentials.getAPIKey() != null);
    }

    @Test
    public void testConstructor02() throws IOException {
        try {
            Properties props = new Properties();
            new TreasureDataCredentials(props);
            fail();
        } catch (Throwable t) {
            assertTrue(t instanceof NullPointerException);
        }
    }
}
