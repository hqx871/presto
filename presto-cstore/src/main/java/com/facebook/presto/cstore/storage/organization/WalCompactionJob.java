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
package com.facebook.presto.cstore.storage.organization;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.TableColumn;
import com.facebook.presto.cstore.metadata.TableMetadata;
import com.facebook.presto.cstore.storage.StorageManager;
import com.facebook.presto.cstore.storage.WriteAheadLogManager;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class WalCompactionJob
        implements Runnable
{
    private static final Logger log = Logger.get(WalCompactionJob.class);

    private final MetadataDao metadataDao;
    private final ShardManager shardManager;
    private final ShardCompactor compactor;
    private final WalCompactionSet organizationSet;
    private final StorageManager storageManager;
    private final WriteAheadLogManager walManager;

    public WalCompactionJob(WalCompactionSet organizationSet, MetadataDao metadataDao, ShardManager shardManager,
            ShardCompactor compactor, StorageManager storageManager, WriteAheadLogManager walManager)
    {
        this.metadataDao = requireNonNull(metadataDao, "metadataDao is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.organizationSet = requireNonNull(organizationSet, "organizationSet is null");
        this.storageManager = storageManager;
        this.walManager = walManager;
    }

    @Override
    public void run()
    {
        try {
            runJob(organizationSet.getTableId(), organizationSet.getBucketNumber(), organizationSet.getShardUuids());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runJob(long tableId, OptionalInt bucketNumber, List<UUID> uuids)
            throws IOException
    {
        long transactionId = shardManager.beginTransaction();
        try {
            runJob(transactionId, bucketNumber, tableId, uuids);
        }
        catch (Throwable e) {
            shardManager.rollbackTransaction(transactionId);
            throw e;
        }
    }

    private void runJob(long transactionId, OptionalInt bucketNumber, long tableId, List<UUID> uuids)
            throws IOException
    {
        TableMetadata metadata = getTableMetadata(tableId);

        List<ShardInfo> newShards = performCompaction(transactionId, bucketNumber, uuids, metadata);
        log.info("Compacted shards %s into %s for table: %s",
                uuids,
                newShards.stream().map(ShardInfo::getShardUuid).collect(toList()),
                tableId);
        shardManager.replaceShardUuids(transactionId, tableId, metadata.getColumns(), Sets.newHashSet(uuids), newShards, OptionalLong.empty());
        organizationSet.getShardUuids().forEach(storageManager::deleteShard);
        walManager.compact(organizationSet.getWalUuid(), organizationSet.getWalBaseOffset(), organizationSet.getTransactionEnd());
    }

    private TableMetadata getTableMetadata(long tableId)
    {
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);

        List<Long> sortColumnIds = sortColumns.stream()
                .map(TableColumn::getColumnId)
                .collect(toList());

        List<TableColumn> columns = metadataDao.listTableColumns(tableId);
        return new TableMetadata(tableId, columns, sortColumnIds);
    }

    private List<ShardInfo> performCompaction(long transactionId, OptionalInt bucketNumber, List<UUID> uuids, TableMetadata tableMetadata)
            throws IOException
    {
        if (tableMetadata.getSortColumnIds().isEmpty()) {
            return compactor.compact(transactionId, bucketNumber, uuids, tableMetadata);
        }
        else {
            return compactor.compactSorted(
                    transactionId,
                    bucketNumber,
                    uuids,
                    tableMetadata,
                    nCopies(tableMetadata.getSortColumnIds().size(), ASC_NULLS_FIRST));
        }
    }

    public int getPriority()
    {
        return organizationSet.getPriority();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("metadataDao", metadataDao)
                .add("shardManager", shardManager)
                .add("compactor", compactor)
                .add("organizationSet", organizationSet)
                .add("priority", organizationSet.getPriority())
                .toString();
    }
}
