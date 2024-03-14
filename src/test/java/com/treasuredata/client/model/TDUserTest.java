package com.treasuredata.client.model;

import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TDUserTest
{
    private static final String USER_JSON = "{\"id\":300,\"first_name\":\"Freda\",\"last_name\":\"Schuster\",\"email\":\"1elvie.hackett@example.com\",\"phone\":\"(650) 469-3644\",\"gravatar_url\":\"https://secure.gravatar.com/avatar/0e36aa63098c5a05b4dde8ac867eb116?size=80\",\"administrator\":true,\"created_at\":\"2016-06-09T01:34:59Z\",\"updated_at\":\"2016-06-09T01:34:59Z\",\"name\":\"Freda Schuster\",\"account_owner\":true}";

    private static final String USER_WITHOUT_OPTIONAL_FIELDS_JSON = "{\"id\":300,\"first_name\":null,\"last_name\":null,\"email\":\"1elvie.hackett@example.com\",\"phone\":null,\"gravatar_url\":\"https://secure.gravatar.com/avatar/0e36aa63098c5a05b4dde8ac867eb116?size=80\",\"administrator\":true,\"created_at\":\"2016-06-09T01:34:59Z\",\"updated_at\":\"2016-06-09T01:34:59Z\",\"name\":\"Freda Schuster\",\"account_owner\":true}";

    @Test
    public void json()
            throws Exception
    {
        TDUser user = ObjectMappers.compactMapper().readValue(USER_JSON, TDUser.class);
        String serialized = ObjectMappers.compactMapper().writeValueAsString(user);
        TDUser parsed = ObjectMappers.compactMapper().readValue(serialized, TDUser.class);
        assertThat(parsed, is(user));
    }

    @Test
    public void jsonOptional()
            throws Exception
    {
        TDUser user = ObjectMappers.compactMapper().readValue(USER_WITHOUT_OPTIONAL_FIELDS_JSON, TDUser.class);
        String serialized = ObjectMappers.compactMapper().writeValueAsString(user);
        TDUser parsed = ObjectMappers.compactMapper().readValue(serialized, TDUser.class);
        assertThat(parsed, is(user));
    }
}
