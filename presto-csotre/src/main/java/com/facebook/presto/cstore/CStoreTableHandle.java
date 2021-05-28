package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.relation.RowExpression;

import javax.annotation.Nullable;

public class CStoreTableHandle
        implements ConnectorTableHandle
{
    private final String schema;
    private final String table;
    @Nullable
    private final RowExpression filter;

    public CStoreTableHandle(String schema, String table, @Nullable RowExpression filter)
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

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }
}
