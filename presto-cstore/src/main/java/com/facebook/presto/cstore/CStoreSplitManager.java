package com.facebook.presto.cstore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.ArrayList;
import java.util.List;

public class CStoreSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(CStoreSplitManager.class);

    private final String connectorId;

    public CStoreSplitManager(CStoreConnectorId connectorId)
    {
        this.connectorId = connectorId.toString();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        //todo use real store meta
        final String directory = "";
        CStoreTableLayoutHandle tableLayout = (CStoreTableLayoutHandle) layout;
        CStoreTableHandle tableHandle = tableLayout.getTable();
        List<CStoreSplit> splits = new ArrayList<>();
        splits.add(new CStoreSplit(connectorId, directory + "20210528", tableHandle.getFilter()));
        return new FixedSplitSource(splits);
    }
}
