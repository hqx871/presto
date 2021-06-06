package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public class CStoreTableHandleV2
        extends CStoreTableHandle
        implements ConnectorTableHandle
{
    private final String schema;
    private final String table;
    private final PlanNode query;

    @JsonCreator
    public CStoreTableHandleV2(@JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("query") PlanNode query)
    {
        super(schema, table, null);
        this.schema = schema;
        this.table = table;
        this.query = query;
    }

    @JsonProperty
    public PlanNode getQuery()
    {
        return query;
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schema", schema)
                .add("table", table)
                .add("query", query)
                .toString();
    }
}
