package com.treasuredata.client.model;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TDUserTest
{
    private static final String USER_JSON = "{\"id\":300,\"first_name\":\"Freda\",\"last_name\":\"Schuster\",\"email\":\"1elvie.hackett@example.com\",\"phone\":\"(650) 469-3644\",\"gravatar_url\":\"https://secure.gravatar.com/avatar/0e36aa63098c5a05b4dde8ac867eb116?size=80\",\"administrator\":true,\"created_at\":\"2016-06-09T01:34:59Z\",\"updated_at\":\"2016-06-09T01:34:59Z\",\"name\":\"Freda Schuster\",\"account_owner\":true}";

    @Test
    public void json()
            throws Exception
    {
        TDUser user = ObjectMappers.compactMapper().readValue(USER_JSON, TDUser.class);
        JsonNode parsed = ObjectMappers.compactMapper().readTree(USER_JSON);
        String serialized = ObjectMappers.compactMapper().writeValueAsString(user);
        assertThat(ObjectMappers.compactMapper().readTree(serialized), is(parsed));
    }
}
