package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;

public class CStoreTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final CStoreTableHandle table;

    public CStoreTableLayoutHandle(CStoreTableHandle table)
    {
        this.table = table;
    }

    public CStoreTableHandle getTable()
    {
        return table;
    }
}
