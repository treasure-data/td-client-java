package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class TDUserList
{
    private final List<TDUser> users;

    @JsonCreator
    TDUserList(@JsonProperty("users") List<TDUser> users)
    {
        this.users = Objects.requireNonNull(users, "users");
    }

    @JsonProperty("users")
    public List<TDUser> getUsers()
    {
        return users;
    }

    static TDUserList of(List<TDUser> users)
    {
        return new TDUserList(users);
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

        TDUserList that = (TDUserList) o;

        return users != null ? users.equals(that.users) : that.users == null;
    }

    @Override
    public int hashCode()
    {
        return users != null ? users.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "TDUserList{" +
                "users=" + users +
                '}';
    }
}
