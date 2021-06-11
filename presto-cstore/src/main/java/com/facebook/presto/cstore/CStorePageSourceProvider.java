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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cstore.storage.ReaderAttributes;
import com.facebook.presto.cstore.storage.StorageManager;
import com.facebook.presto.cstore.util.ConcatPageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;

import javax.inject.Inject;

import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class CStorePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final StorageManager storageManager;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;

    @Inject
    public CStorePageSourceProvider(StorageManager storageManager, TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager, StandardFunctionResolution standardFunctionResolution)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.standardFunctionResolution = standardFunctionResolution;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split,
            ConnectorTableLayoutHandle layout, List<ColumnHandle> columns, SplitContext splitContext)
    {
        CStoreSplit cStoreSplit = (CStoreSplit) split;
        OptionalInt bucketNumber = cStoreSplit.getBucketNumber();
        TupleDomain<CStoreColumnHandle> predicate = cStoreSplit.getEffectivePredicate();
        ReaderAttributes attributes = ReaderAttributes.from(session);
        OptionalLong transactionId = cStoreSplit.getTransactionId();
        List<CStoreColumnHandle> columnHandles = columns.stream().map(e -> (CStoreColumnHandle) e).collect(Collectors.toList());

        List<ConnectorPageSource> cStorePageSources = cStoreSplit.getShardUuids().stream()
                .map(shardUuid -> {
                    return storageManager.getPageSource(shardUuid, bucketNumber, columnHandles, predicate, cStoreSplit.getFilter(), transactionId);
                })
                .collect(Collectors.toList());
        if (cStorePageSources.size() == 1) {
            return cStorePageSources.get(0);
        }
        else {
            return new ConcatPageSource(cStorePageSources.iterator());
        }
    }
}
