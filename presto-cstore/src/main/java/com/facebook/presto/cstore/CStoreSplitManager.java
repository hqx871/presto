package com.facebook.presto.cstore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;
import org.apache.cstore.manage.CStoreDatabase;
import org.apache.cstore.meta.TableMeta;

import java.util.ArrayList;
import java.util.List;

public class CStoreSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(CStoreSplitManager.class);

    private final String connectorId;
    private final CStoreDatabase database;

    @Inject
    public CStoreSplitManager(CStoreConnectorId connectorId, CStoreDatabase database)
    {
        this.connectorId = connectorId.toString();
        this.database = database;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        CStoreTableLayoutHandle tableLayout = (CStoreTableLayoutHandle) layout;
        CStoreTableHandle tableHandle = tableLayout.getTable();
        List<CStoreSplit> splits = new ArrayList<>();
        TableMeta tableMeta = database.getTableMeta(tableHandle.getSchema(), tableHandle.getTable());
        String path = database.getTablePath(tableHandle.getSchema(), tableHandle.getTable());
        splits.add(new CStoreSplit(connectorId, path, tableMeta.getRowCnt(), tableHandle.getFilter()));
        return new FixedSplitSource(splits);
    }
}
