package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TDUser
{
    private final String id;
    private final String name;
    private final String firstName;
    private final String lastName;
    private final String email;
    private final String phone;
    private final String gravatarUrl;
    private final boolean administrator;
    private final String createdAt;
    private final String updatedAt;
    private final boolean accountOwner;

    TDUser(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("first_name") String firstName,
            @JsonProperty("last_name") String lastName,
            @JsonProperty("email") String email,
            @JsonProperty("phone") String phone,
            @JsonProperty("gravatar_url") String gravatarUrl,
            @JsonProperty("administrator") boolean administrator,
            @JsonProperty("created_at") String createdAt,
            @JsonProperty("updated_at") String updatedAt,
            @JsonProperty("account_owner") boolean accountOwner
    )
    {
        this.id = id;
        this.name = name;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phone = phone;
        this.gravatarUrl = gravatarUrl;
        this.administrator = administrator;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.accountOwner = accountOwner;
    }

    public String getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public String getEmail()
    {
        return email;
    }

    public String getPhone()
    {
        return phone;
    }

    public String getGravatarUrl()
    {
        return gravatarUrl;
    }

    public boolean isAdministrator()
    {
        return administrator;
    }

    public String getCreatedAt()
    {
        return createdAt;
    }

    public String getUpdatedAt()
    {
        return updatedAt;
    }

    public boolean isAccountOwner()
    {
        return accountOwner;
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

        if (administrator != tdUser.administrator) {
            return false;
        }
        if (accountOwner != tdUser.accountOwner) {
            return false;
        }
        if (id != null ? !id.equals(tdUser.id) : tdUser.id != null) {
            return false;
        }
        if (name != null ? !name.equals(tdUser.name) : tdUser.name != null) {
            return false;
        }
        if (firstName != null ? !firstName.equals(tdUser.firstName) : tdUser.firstName != null) {
            return false;
        }
        if (lastName != null ? !lastName.equals(tdUser.lastName) : tdUser.lastName != null) {
            return false;
        }
        if (email != null ? !email.equals(tdUser.email) : tdUser.email != null) {
            return false;
        }
        if (phone != null ? !phone.equals(tdUser.phone) : tdUser.phone != null) {
            return false;
        }
        if (gravatarUrl != null ? !gravatarUrl.equals(tdUser.gravatarUrl) : tdUser.gravatarUrl != null) {
            return false;
        }
        if (createdAt != null ? !createdAt.equals(tdUser.createdAt) : tdUser.createdAt != null) {
            return false;
        }
        return updatedAt != null ? updatedAt.equals(tdUser.updatedAt) : tdUser.updatedAt == null;
    }

    @Override
    public int hashCode()
    {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (email != null ? email.hashCode() : 0);
        result = 31 * result + (phone != null ? phone.hashCode() : 0);
        result = 31 * result + (gravatarUrl != null ? gravatarUrl.hashCode() : 0);
        result = 31 * result + (administrator ? 1 : 0);
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        result = 31 * result + (updatedAt != null ? updatedAt.hashCode() : 0);
        result = 31 * result + (accountOwner ? 1 : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "TDUser{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", email='" + email + '\'' +
                ", phone='" + phone + '\'' +
                ", gravatarUrl='" + gravatarUrl + '\'' +
                ", administrator=" + administrator +
                ", createdAt='" + createdAt + '\'' +
                ", updatedAt='" + updatedAt + '\'' +
                ", accountOwner=" + accountOwner +
                '}';
    }
}
