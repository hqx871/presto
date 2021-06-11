package com.facebook.presto.cstore;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CStoreTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final CStoreTableHandle table;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<CStorePartitioningHandle> partitioning;
    private final RowExpression filter;

    @JsonCreator
    public CStoreTableLayoutHandle(
            @JsonProperty("table") CStoreTableHandle table,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("partitioning") Optional<CStorePartitioningHandle> partitioning,
            @Nullable @JsonProperty("filter") RowExpression filter)
    {
        this.table = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.filter = filter;
    }

    @JsonProperty
    public CStoreTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<CStorePartitioningHandle> getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public RowExpression getFilter()
    {
        return filter;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
