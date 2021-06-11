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

import com.facebook.presto.cstore.storage.StorageManager;
import com.facebook.presto.cstore.storage.StorageManagerConfig;
import com.facebook.presto.cstore.storage.organization.TemporalFunction;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.cstore.CStoreSessionProperties.getWriterMaxBufferSize;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CStorePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final TemporalFunction temporalFunction;
    private final int maxAllowedFilesPerWriter;

    @Inject
    public CStorePageSinkProvider(StorageManager storageManager, PageSorter pageSorter, TemporalFunction temporalFunction, StorageManagerConfig config)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.temporalFunction = requireNonNull(temporalFunction, "temporalFunction is null");
        this.maxAllowedFilesPerWriter = config.getMaxAllowedFilesPerWriter();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, PageSinkContext pageSinkContext)
    {
        checkArgument(!pageSinkContext.isCommitRequired(), "Raptor connector does not support page sink commit");

        RaptorOutputTableHandle handle = (RaptorOutputTableHandle) tableHandle;
        return new CStorePageSink(
                new HdfsContext(session, handle.getSchemaName(), handle.getTableName()),
                pageSorter,
                storageManager,
                handle.getColumnHandles(),
                temporalFunction,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                getWriterMaxBufferSize(session),
                maxAllowedFilesPerWriter);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, PageSinkContext pageSinkContext)
    {
        checkArgument(!pageSinkContext.isCommitRequired(), "Raptor connector does not support page sink commit");

        RaptorInsertTableHandle handle = (RaptorInsertTableHandle) tableHandle;
        return new CStorePageSink(
                new HdfsContext(session),
                pageSorter,
                storageManager,
                handle.getColumnHandles(),
                temporalFunction,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                getWriterMaxBufferSize(session),
                maxAllowedFilesPerWriter);
    }

    private static List<Long> toColumnIds(List<CStoreColumnHandle> columnHandles)
    {
        return columnHandles.stream().map(CStoreColumnHandle::getColumnId).collect(toList());
    }
}
