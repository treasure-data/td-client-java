package com.treasuredata.client.model;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TDUserTest
{
    private static final String USER_JSON = "{\"id\":\"298\",\"first_name\":\"Muhammad\",\"last_name\":\"Bruen\",\"email\":\"1elroy_vonrueden@example.net\",\"phone\":\"(650) 469-3644\",\"gravatar_url\":\"https://secure.gravatar.com/avatar/0e66e45a5283c2fff6373f0a34c734ae?size=80\",\"administrator\":true,\"created_at\":\"2016-06-08T05:22:56Z\",\"updated_at\":\"2016-06-08T05:22:56Z\",\"name\":\"Muhammad Bruen\",\"account_owner\":true}";

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