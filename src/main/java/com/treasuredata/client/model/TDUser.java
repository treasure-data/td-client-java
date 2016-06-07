package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TDUser
{
    private final String name;
    private final String email;

    TDUser(@JsonProperty("name") String name,
            @JsonProperty("email") String email)
    {
        this.name = name;
        this.email = email;
    }

    public String getName()
    {
        return name;
    }

    public String getEmail()
    {
        return email;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TDUser tdUser = (TDUser) o;

        if (name != null ? !name.equals(tdUser.name) : tdUser.name != null) {
            return false;
        }
        return email != null ? email.equals(tdUser.email) : tdUser.email == null;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (email != null ? email.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "TDUser{" +
                "name='" + name + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
