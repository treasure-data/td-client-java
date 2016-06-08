package com.treasuredata.client.model;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class TDUserTest
{
    private static final String ID = "test-id";
    private static final String NAME = "test-name";
    private static final String FIRST_NAME = "test-first_name";
    private static final String LAST_NAME = "test-last_name";
    private static final String EMAIL = "test-EMAIL";
    private static final String PHONE = "test-PHONE";
    private static final String GRAVATAR_URL = "test-gravatar_url";
    private static final boolean ADMINISTRATOR = true;
    private static final String CREATED_AT = "test-created_at";
    private static final String UPDATED_AT = "test-updated_at";
    private static final boolean ACCOUNT_OWNER = true;

    private static final TDUser USER = new TDUser(ID, NAME, FIRST_NAME, LAST_NAME, EMAIL, PHONE, GRAVATAR_URL, ADMINISTRATOR, CREATED_AT, UPDATED_AT, ACCOUNT_OWNER);

    @Test
    public void fields()
            throws Exception
    {
        assertThat(USER.getId(), is(ID));
        assertThat(USER.getName(), is(NAME));
        assertThat(USER.getFirstName(), is(FIRST_NAME));
        assertThat(USER.getLastName(), is(LAST_NAME));
        assertThat(USER.getEmail(), is(EMAIL));
        assertThat(USER.getPhone(), is(PHONE));
        assertThat(USER.getGravatarUrl(), is(GRAVATAR_URL));
        assertThat(USER.isAdministrator(), is(ADMINISTRATOR));
        assertThat(USER.getCreatedAt(), is(CREATED_AT));
        assertThat(USER.getUpdatedAt(), is(UPDATED_AT));
        assertThat(USER.isAccountOwner(), is(ACCOUNT_OWNER));
    }

    @Test
    public void json()
            throws Exception
    {
        String json = ObjectMappers.compactMapper().writeValueAsString(USER);

        TDUser parsed = ObjectMappers.compactMapper().readValue(json, TDUser.class);

        assertThat(parsed.getId(), is(ID));
        assertThat(parsed.getName(), is(NAME));
        assertThat(parsed.getFirstName(), is(FIRST_NAME));
        assertThat(parsed.getLastName(), is(LAST_NAME));
        assertThat(parsed.getEmail(), is(EMAIL));
        assertThat(parsed.getPhone(), is(PHONE));
        assertThat(parsed.getGravatarUrl(), is(GRAVATAR_URL));
        assertThat(parsed.isAdministrator(), is(ADMINISTRATOR));
        assertThat(parsed.getCreatedAt(), is(CREATED_AT));
        assertThat(parsed.getUpdatedAt(), is(UPDATED_AT));
        assertThat(parsed.isAccountOwner(), is(ACCOUNT_OWNER));

        assertThat(parsed, is(USER));
    }

    @Test
    public void equals()
            throws Exception
    {
        assertThat(USER.hashCode(), is(USER.hashCode()));
        assertThat(USER, is(USER));

        TDUser otherUser = new TDUser(ID + 1, NAME, FIRST_NAME, LAST_NAME, EMAIL, PHONE, GRAVATAR_URL, ADMINISTRATOR, CREATED_AT, UPDATED_AT, ACCOUNT_OWNER);
        assertThat(USER, is(not(otherUser)));
    }
}