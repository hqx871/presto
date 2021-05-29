package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

public class CStoreSplit
        implements ConnectorSplit
{
    private final List<HostAddress> localhost = Collections.singletonList(new HostAddress("localhost", 0));

    private final String schema;
    private final String table;
    private final String path;
    @Nullable
    private final RowExpression filter;
    private final String connectorId;
    private final int rowCount;

    @JsonCreator
    public CStoreSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("path") String path,
            @JsonProperty("rowCount") int rowCount,
            @JsonProperty("filter") @Nullable RowExpression filter)
    {
        this.schema = schema;
        this.table = table;
        this.filter = filter;
        this.connectorId = connectorId;
        this.path = path;
        this.rowCount = rowCount;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return localhost;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @Nullable
    @JsonProperty
    public RowExpression getFilter()
    {
        return filter;
    }

    @JsonProperty
    public int getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }
}
