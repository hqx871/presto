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

import com.facebook.presto.common.block.SortOrder;
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
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

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
        checkArgument(!pageSinkContext.isCommitRequired(), "CStore connector does not support page sink commit");

        CStoreOutputTableHandle handle = (CStoreOutputTableHandle) tableHandle;
        return doCreatePageSink(new HdfsContext(session, handle.getSchemaName(), handle.getTableName()),
                handle.getBucketCount(),
                handle.getTemporalColumnHandle().isPresent() ? OptionalInt.of((int) handle.getTemporalColumnHandle().get().getColumnId()) : OptionalInt.empty(),
                handle.getTransactionId(), handle.getColumnHandles(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getTemporalColumnHandle(),
                toColumnIds(handle.getBucketColumnHandles()),
                getWriterMaxBufferSize(session),
                OptionalLong.empty());
    }

    private ConnectorPageSink doCreatePageSink(HdfsContext hdfsContext, OptionalInt bucketCount, OptionalInt temporalColumnIndex,
            long transactionId, List<CStoreColumnHandle> columnHandles, List<Long> sortFields, List<SortOrder> sortOrders,
            Optional<CStoreColumnHandle> temporalColumn, List<Long> bucketFields, DataSize maxBufferBytes, OptionalLong tableId)
    {
        CStorePageSinkFactory sink = (day, bucketNumber) -> {
            if (tableId.isPresent()) {
                return storageManager.createStoragePageBufferSink(tableId.getAsLong(), day, transactionId, bucketNumber,
                        columnHandles, sortFields, sortOrders, false);
            }
            else {
                return storageManager.createStoragePageFileSink(transactionId, bucketNumber, columnHandles, false);
            }
        };

        if (bucketCount.isPresent() || temporalColumnIndex.isPresent()) {
            final CStorePageSinkFactory receiver = sink;
            final CStorePageSinkFactory stash = (day, bucketNumber) -> new CStoreStashPageSink(columnHandles, receiver.create(day, bucketNumber));
            sink = (day, bucketNumber) -> new CStoreBucketPageSink(columnHandles, temporalFunction, bucketCount, bucketFields, temporalColumn, stash);
        }
        return sink.create(OptionalInt.empty(), OptionalInt.empty());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, PageSinkContext pageSinkContext)
    {
        checkArgument(!pageSinkContext.isCommitRequired(), "CStore connector does not support page sink commit");

        CStoreInsertTableHandle handle = (CStoreInsertTableHandle) tableHandle;
        return doCreatePageSink(new HdfsContext(session),
                handle.getBucketCount(),
                handle.getTemporalColumnHandle().isPresent() ? OptionalInt.of((int) handle.getTemporalColumnHandle().get().getColumnId()) : OptionalInt.empty(),
                handle.getTransactionId(), handle.getColumnHandles(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getTemporalColumnHandle(),
                toColumnIds(handle.getBucketColumnHandles()),
                getWriterMaxBufferSize(session),
                OptionalLong.of(handle.getTableId()));
    }

    private static List<Long> toColumnIds(List<CStoreColumnHandle> columnHandles)
    {
        return columnHandles.stream().map(CStoreColumnHandle::getColumnId).collect(toList());
    }
}
