package com.facebook.presto.cstore;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.List;

public class CStorePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final String connectorId;

    @Inject
    public CStorePageSourceProvider(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        CStoreSplit storeSplit = (CStoreSplit) split;
        CStoreColumnHandle[] columnHandles = columns.stream()
                .map(e -> (CStoreColumnHandle) e)
                .toArray(CStoreColumnHandle[]::new);
        return new CStorePageSource(storeSplit, columnHandles, storeSplit.getFilter());
    }
}
