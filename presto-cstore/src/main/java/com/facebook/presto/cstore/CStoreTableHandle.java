package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class CStoreTableHandle
        implements ConnectorTableHandle
{
    private final String schema;
    private final String table;
    @Nullable
    private final RowExpression filter;

    @JsonCreator
    public CStoreTableHandle(@JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("filter") @Nullable RowExpression filter)
    {
        this.schema = schema;
        this.table = table;
        this.filter = filter;
    }

    @Nullable
    public RowExpression getFilter()
    {
        return filter;
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
