package com.facebook.presto.cstore;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.inject.Inject;
import org.apache.cstore.manage.CStoreDatabase;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class CStorePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final String connectorId;
    private final CStoreDatabase database;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;

    @Inject
    public CStorePageSourceProvider(CStoreConnectorId connectorId,
            CStoreDatabase database,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.database = database;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        CStoreSplit storeSplit = (CStoreSplit) split;
        CStoreColumnHandle[] columnHandles = columns.stream()
                .map(e -> (CStoreColumnHandle) e)
                .toArray(CStoreColumnHandle[]::new);
        return new CStorePageSource(database, typeManager, functionMetadataManager, standardFunctionResolution, session, storeSplit, columnHandles, storeSplit.getFilter());
    }
}
