package com.facebook.presto.cstore;

import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class CStoreDeleteTableHandle
        extends CStoreTableHandle
{
    private final List<CStoreColumnHandle> columns;

    @JsonCreator
    public CStoreDeleteTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("distributionId") OptionalLong distributionId,
            @JsonProperty("distributionName") Optional<String> distributionName,
            @JsonProperty("bucketCount") OptionalInt bucketCount,
            @JsonProperty("organized") boolean organized,
            @JsonProperty("transactionId") OptionalLong transactionId,
            @JsonProperty("columns") List<CStoreColumnHandle> columns,
            @JsonProperty("delete") boolean delete,
            @JsonProperty("filter") RowExpression filter)
    {
        super(connectorId, schemaName, tableName, tableId, distributionId, distributionName, bucketCount, organized, transactionId, columns, delete, filter);
        this.columns = requireNonNull(columns, "columns is null");
    }

    @JsonProperty
    public List<CStoreColumnHandle> getColumns()
    {
        return columns;
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
        if (!super.equals(o)) {
            return false;
        }
        CStoreDeleteTableHandle that = (CStoreDeleteTableHandle) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), columns);
    }
}
