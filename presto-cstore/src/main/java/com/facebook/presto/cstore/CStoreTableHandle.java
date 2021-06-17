package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.presto.cstore.util.MetadataUtil.checkSchemaName;
import static com.facebook.presto.cstore.util.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CStoreTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final long tableId;
    private final OptionalLong distributionId;
    private final Optional<String> distributionName;
    private final OptionalInt bucketCount;
    private final boolean organized;
    private OptionalLong transactionId;
    private final List<CStoreColumnHandle> columns;
    private boolean delete;
    @Nullable
    private RowExpression filter;

    @JsonCreator
    public CStoreTableHandle(
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
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = checkSchemaName(schemaName);
        this.tableName = checkTableName(tableName);

        checkArgument(tableId > 0, "tableId must be greater than zero");
        this.tableId = tableId;

        this.distributionName = requireNonNull(distributionName, "distributionName is null");
        this.distributionId = requireNonNull(distributionId, "distributionId is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.organized = organized;
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.columns = requireNonNull(columns, "columns is null");

        this.delete = delete;
        this.filter = filter;
    }

    public boolean isBucketed()
    {
        return this.distributionId.isPresent();
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public OptionalLong getDistributionId()
    {
        return distributionId;
    }

    @JsonProperty
    public Optional<String> getDistributionName()
    {
        return distributionName;
    }

    @JsonProperty
    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public boolean isOrganized()
    {
        return organized;
    }

    @JsonProperty
    public OptionalLong getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public List<CStoreColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public boolean isDelete()
    {
        return delete;
    }

    public void setDelete(boolean delete)
    {
        this.delete = delete;
    }

    public void setTransactionId(OptionalLong transactionId)
    {
        this.transactionId = transactionId;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + schemaName + ":" + tableName + ":" + tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableId, filter);
    }

    @JsonProperty
    public RowExpression getFilter()
    {
        return filter;
    }

    public void setFilter(@Nullable RowExpression filter)
    {
        this.filter = filter;
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
        CStoreTableHandle other = (CStoreTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.tableId, other.tableId) &&
                Objects.equals(this.filter, other.getFilter());
    }
}
