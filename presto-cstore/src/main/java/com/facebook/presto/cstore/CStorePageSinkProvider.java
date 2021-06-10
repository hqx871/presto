/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cstore;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.annotations.VisibleForTesting;
import github.cstore.CStoreDatabase;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class CStorePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final CStoreDatabase database;
    private final HostAddress currentHostAddress;

    @Inject
    public CStorePageSinkProvider(CStoreDatabase database, NodeManager nodeManager)
    {
        this(database, requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort());
    }

    @VisibleForTesting
    public CStorePageSinkProvider(CStoreDatabase database, HostAddress currentHostAddress)
    {
        this.database = requireNonNull(database, "pagesStore is null");
        this.currentHostAddress = requireNonNull(currentHostAddress, "currentHostAddress is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        //checkArgument(!pageSinkContext.isCommitRequired(), "Memory connector does not support page sink commit");

        CStoreOutputTableHandle cStoreOutputTableHandle = (CStoreOutputTableHandle) outputTableHandle;
        CStoreTableHandle tableHandle = cStoreOutputTableHandle.getTable();
        //long tableId = tableHandle.getTableId();
        //checkState(CStoreOutputTableHandle.getActiveTableIds().contains(tableId));

        database.createStagingTable(tableHandle.getSchema(), tableHandle.getTable());
        return new CStorePageSink(database, currentHostAddress, tableHandle, cStoreOutputTableHandle.getColumns());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        //checkArgument(!pageSinkContext.isCommitRequired(), "Memory connector does not support page sink commit");

        CStoreInsertTableHandle cStoreInsertTableHandle = (CStoreInsertTableHandle) insertTableHandle;
        CStoreTableHandle tableHandle = cStoreInsertTableHandle.getTable();
        //long tableId = tableHandle.getTableId();
        //checkState(memoryInsertTableHandle.getActiveTableIds().contains(tableId));

        database.createStagingTable(tableHandle.getSchema(), tableHandle.getTable());
        return new CStorePageSink(database, currentHostAddress, tableHandle, cStoreInsertTableHandle.getColumns());
    }
}
