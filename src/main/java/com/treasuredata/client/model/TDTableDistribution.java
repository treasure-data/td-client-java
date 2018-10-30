package com.treasuredata.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.List;

/**
 * TDTableDistribution provides the information about custom partitioning of the table.
 */
public class TDTableDistribution
{
    private final long userTableId;
    private final long bucketCount;
    private final String partitionFunction;
    private final List<TDColumn> columns;

    @JsonCreator
    public TDTableDistribution(
            @JsonProperty("user_table_id") long userTableId,
            @JsonProperty("bucket_count") long bucketCount,
            @JsonProperty("partition_function") String partitionFunction,
            @JsonProperty("columns") List<TDColumn> columns)
    {
        this.userTableId = userTableId;
        this.bucketCount = bucketCount;
        this.partitionFunction = partitionFunction;
        this.columns = columns;
    }

    @JsonProperty
    public long getUserTableId()
    {
        return userTableId;
    }

    /**
     * The maximum number of buckets of UDP table.
     */
    @JsonProperty
    public long getBucketCount()
    {
        return bucketCount;
    }

    /**
     * Hash function to calculate the partitioning key.
     */
    @JsonProperty
    public String getPartitionFunction()
    {
        return partitionFunction;
    }

    /**
     * Columns used for partitioning key in UDP table
     */
    @JsonProperty
    public List<TDColumn> getColumns()
    {
        return columns;
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
                Objects.equal(this.partitionFunction, other.partitionFunction) &&
                Objects.equal(this.columns, other.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(userTableId, bucketCount, partitionFunction);
    }

    @Override
    public String toString()
    {
        return String.format("userTableId: %s, bucketCount: %s, partitionFunction: %s, columns: %s",
                userTableId, bucketCount, partitionFunction, columns);
    }
}
