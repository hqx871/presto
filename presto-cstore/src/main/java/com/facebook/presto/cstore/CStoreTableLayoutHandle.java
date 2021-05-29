package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public class CStoreTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final CStoreTableHandle table;

    @JsonCreator
    public CStoreTableLayoutHandle(@JsonProperty("table") CStoreTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public CStoreTableHandle getTable()
    {
        return table;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .toString();
    }
}
