package com.facebook.presto.cstore;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

public class CStoreConnector
        implements Connector
{
    private final CStoreMetadata metadata;
    private final CStoreSplitManager splitManager;
    private final CStorePageSourceProvider pageSourceProvider;
    private final CStorePlanOptimizer planOptimizer;
    private final CStorePageSinkProvider pageSinkProvider;

    @Inject
    public CStoreConnector(CStoreMetadata metadata,
            CStoreSplitManager splitManager,
            CStorePageSourceProvider pageSourceProvider,
            CStorePlanOptimizer planOptimizer,
            CStorePageSinkProvider pageSinkProvider)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.planOptimizer = planOptimizer;
        this.pageSinkProvider = pageSinkProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return CStoreTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider()
    {
        return new CStorePlanOptimizerProvider(planOptimizer);
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }
}
