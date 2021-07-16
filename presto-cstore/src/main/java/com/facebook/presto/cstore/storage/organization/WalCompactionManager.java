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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.metadata.ForMetadata;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.ShardMetadata;
import com.facebook.presto.cstore.metadata.Table;
import com.facebook.presto.cstore.metadata.TableColumn;
import com.facebook.presto.cstore.metadata.WalMetadata;
import com.facebook.presto.cstore.storage.ActionType;
import com.facebook.presto.cstore.storage.MemoryShardManager;
import com.facebook.presto.cstore.storage.StorageManagerConfig;
import com.facebook.presto.cstore.storage.WriteAheadLogManager;
import com.facebook.presto.cstore.storage.WriteAheadLogReader;
import com.facebook.presto.cstore.storage.WriteAheadLogRecord;
import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.cstore.storage.organization.ShardOrganizerUtil.getOrganizationEligibleShards;
import static com.facebook.presto.cstore.util.DatabaseUtil.onDemandDao;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class WalCompactionManager
{
    private static final Logger log = Logger.get(WalCompactionManager.class);

    private static final double FILL_FACTOR = 0.75;

    private final ScheduledExecutorService compactionDiscoveryService = newScheduledThreadPool(1, daemonThreadsNamed("shard-compaction-discovery"));

    private final AtomicBoolean discoveryStarted = new AtomicBoolean();
    private final AtomicBoolean shutdown = new AtomicBoolean();

    private final MetadataDao metadataDao;
    private final WalOrganizer organizer;
    private final ShardManager shardManager;
    private final String currentNodeIdentifier;
    private final WalCompactionSetCreator compactionSetCreator;

    private final boolean compactionEnabled;
    private final Duration compactionDiscoveryInterval;
    private final DataSize maxShardSize;
    private final long maxShardRows;
    private final IDBI dbi;
    private final MemoryShardManager memoryShardManager;
    private final WriteAheadLogManager walManager;

    @Inject
    public WalCompactionManager(@ForMetadata IDBI dbi,
            NodeManager nodeManager,
            ShardManager shardManager,
            WalOrganizer organizer,
            TemporalFunction temporalFunction,
            StorageManagerConfig config,
            MemoryShardManager memoryShardManager,
            WriteAheadLogManager walManager)
    {
        this(dbi,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                shardManager,
                organizer,
                temporalFunction,
                config.getCompactionInterval(),
                config.getMaxShardSize(),
                config.getMaxShardRows(),
                config.isCompactionEnabled(),
                memoryShardManager,
                walManager);
    }

    public WalCompactionManager(
            IDBI dbi,
            String currentNodeIdentifier,
            ShardManager shardManager,
            WalOrganizer organizer,
            TemporalFunction temporalFunction,
            Duration compactionDiscoveryInterval,
            DataSize maxShardSize,
            long maxShardRows,
            boolean compactionEnabled,
            MemoryShardManager memoryShardManager, WriteAheadLogManager walManager)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);

        this.currentNodeIdentifier = requireNonNull(currentNodeIdentifier, "currentNodeIdentifier is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.organizer = requireNonNull(organizer, "organizer is null");
        this.compactionDiscoveryInterval = requireNonNull(compactionDiscoveryInterval, "compactionDiscoveryInterval is null");
        this.memoryShardManager = memoryShardManager;
        this.walManager = walManager;

        checkArgument(maxShardSize.toBytes() > 0, "maxShardSize must be > 0");
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;

        this.compactionEnabled = compactionEnabled;
        this.compactionSetCreator = new WalCompactionSetCreator(temporalFunction, maxShardSize, maxShardRows, 2);
    }

    @PostConstruct
    public void start()
    {
        if (!compactionEnabled) {
            return;
        }

        if (!discoveryStarted.getAndSet(true)) {
            startDiscovery();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        if (!compactionEnabled) {
            return;
        }
        shutdown.set(true);
        compactionDiscoveryService.shutdown();
    }

    private void startDiscovery()
    {
        compactionDiscoveryService.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database
                long interval = (long) compactionDiscoveryInterval.convertTo(SECONDS).getValue();
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
                discoverShards();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error discovering shards to compact");
            }
        }, 0, compactionDiscoveryInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void discoverShards()
    {
        log.info("Discovering shards that need compaction...");
        Set<ShardMetadata> allShards = shardManager.getNodeShards(currentNodeIdentifier);
        allShards = allShards.stream().filter(this::needsCompaction).collect(toSet());
        //compactShards(allShards, 0);
    }

    public void commit(UUID walUuid)
    {
        WalMetadata walMetadata = metadataDao.getWal(walUuid);
        WriteAheadLogReader walReader = walManager.getReader(walMetadata);
        Set<UUID> memoryShardUuidSet = new HashSet<>();
        long transactionEnd = 0;
        long baseOffset = walMetadata.getBaseOffset();
        while (walReader.hasNext()) {
            WriteAheadLogRecord logRecord = walReader.next();
            baseOffset += logRecord.getBytesSize();
            UUID shardUuid = logRecord.getShardUuid();
            ActionType actionType = logRecord.getActionType();
            transactionEnd = logRecord.getTransactionId();
            switch (actionType) {
                case COMMIT: {
                    memoryShardUuidSet.add(shardUuid);
                    break;
                }
                case DROP:
                case ROLLBACK: {
                    memoryShardUuidSet.remove(shardUuid);
                    break;
                }
                default:
            }
        }
        Set<ShardMetadata> shardMetadataSet = shardManager.getNodeShards(currentNodeIdentifier);
        shardMetadataSet = shardMetadataSet.stream()
                .filter(shardMetadata -> memoryShardUuidSet.contains(shardMetadata.getShardUuid()))
                .collect(Collectors.toSet());
        compactShards(shardMetadataSet, walUuid, baseOffset, transactionEnd);
    }

    public void compactShards(Set<ShardMetadata> shards, UUID walUuid, long walBaseOffset, long transactionEnd)
    {
        ListMultimap<Long, ShardMetadata> tableShardMultiMap = Multimaps.index(shards, ShardMetadata::getTableId);

        for (Entry<Long, List<ShardMetadata>> entry : Multimaps.asMap(tableShardMultiMap).entrySet()) {
            long tableId = entry.getKey();
            if (!metadataDao.isCompactionEligible(tableId)) {
                continue;
            }
            List<ShardMetadata> tableShards = entry.getValue();
            Set<WalCompactionSet> organizationSets = filterAndCreateCompactionSets(tableId, tableShards, walUuid, walBaseOffset, transactionEnd);
            log.info("Created %s compaction set(s) from %s shards for table ID %s", organizationSets.size(), tableShards.size(), tableId);

            for (WalCompactionSet set : organizationSets) {
                organizer.enqueue(set);
            }
        }
    }

    private Set<WalCompactionSet> filterAndCreateCompactionSets(long tableId, Collection<ShardMetadata> tableShards, UUID walUuid, long walBaseOffset, long transactionEnd)
    {
        Table tableInfo = metadataDao.getTableInformation(tableId);
        OptionalLong temporalColumnId = tableInfo.getTemporalColumnId();
        if (temporalColumnId.isPresent()) {
            TableColumn tableColumn = metadataDao.getTableColumn(tableId, temporalColumnId.getAsLong());
            if (!isValidTemporalColumn(tableId, tableColumn.getDataType())) {
                return ImmutableSet.of();
            }
        }

        Set<ShardMetadata> filteredShards = tableShards.stream()
                .filter(this::needsCompaction)
                .filter(shard -> !organizer.inProgress(shard.getShardUuid()))
                .collect(toSet());

        Collection<ShardIndexInfo> shardIndexInfos = getOrganizationEligibleShards(dbi, metadataDao, tableInfo, filteredShards, false);
        if (tableInfo.getTemporalColumnId().isPresent()) {
            shardIndexInfos = shardIndexInfos.stream()
                    .filter(shard -> shard.getTemporalRange().isPresent())
                    .collect(toSet());
        }

        return compactionSetCreator.createCompactionSets(tableInfo, shardIndexInfos, walUuid, walBaseOffset, transactionEnd);
    }

    private static boolean isValidTemporalColumn(long tableId, Type type)
    {
        if (!type.equals(DATE) && !type.equals(TIMESTAMP)) {
            log.warn("Temporal column type of table ID %s set incorrectly to %s", tableId, type);
            return false;
        }
        return true;
    }

    private boolean needsCompaction(ShardMetadata shard)
    {
        return shard.isMutable();
    }
}
