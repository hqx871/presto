package com.facebook.presto.cstore;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

public class CStoreConnector
        implements Connector
{
    private final CStoreConnectorMetadata metadata;
    private final CStoreSplitManager splitManager;
    private final CStorePageSourceProvider pageSourceProvider;
    private final CStorePlanOptimizer planOptimizer;

    @Inject
    public CStoreConnector(CStoreConnectorMetadata metadata,
            CStoreSplitManager splitManager,
            CStorePageSourceProvider pageSourceProvider,
            CStorePlanOptimizer planOptimizer)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.planOptimizer = planOptimizer;
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
}