package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
}
