package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;

public class CStoreSplit
        implements ConnectorSplit
{
    private final List<HostAddress> localhost = Collections.singletonList(new HostAddress("localhost", 0));
    private final String path;
    @Nullable
    private final RowExpression filter;
    private final String connectorId;

    public CStoreSplit(String connectorId, String path, @Nullable RowExpression filter)
    {
        this.path = path;
        this.filter = filter;
        this.connectorId = connectorId;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
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
}
