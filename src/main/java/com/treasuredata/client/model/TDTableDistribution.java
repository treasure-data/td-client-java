package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 *
 */
public class TDTableDistribution
{
    private final long userTableId;
    private final long bucketCount;
    private final String partitionFunction;

    @JsonCreator
    public TDTableDistribution(
            @JsonProperty("user_table_id") long userTableId,
            @JsonProperty("bucket_count") long bucketCount,
            @JsonProperty("partition_function") String partitionFunction)
    {
        this.userTableId = userTableId;
        this.bucketCount = bucketCount;
        this.partitionFunction = partitionFunction;
    }

    @JsonProperty
    public long getUserTableId()
    {
        return userTableId;
    }

    @JsonProperty
    public long getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public String getPartitionFunction()
    {
        return partitionFunction;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TDTableDistribution other = (TDTableDistribution) obj;
        return Objects.equal(this.userTableId, other.userTableId) &&
                Objects.equal(this.bucketCount, other.bucketCount) &&
                Objects.equal(this.partitionFunction, other.partitionFunction);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(userTableId, bucketCount, partitionFunction);
    }

    @Override
    public String toString()
    {
        return String.format("userTableId: %s, bucketCount: %s, partitionFunction: %s",
                userTableId, bucketCount, partitionFunction);
    }
}
