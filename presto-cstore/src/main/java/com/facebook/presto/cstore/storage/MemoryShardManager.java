package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.ShardMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class MemoryShardManager
{
    private static final long MAX_ROWS = 1_000_000_000;

    private final String nodeId;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final File dataDirectory;
    private final File stagingDirectory;
    private final ShardManager shardManager;
    private final PagesSerdeFactory pagesSerdeFactory;
    private final ConcurrentMap<Object, MemoryShardAccessor> bucketPageBuffers;
    private final ConcurrentMap<UUID, MemoryShardAccessor> pageBuffers;
    private final MetadataDao metadataDao;

    @Inject
    public MemoryShardManager(
            ShardManager shardManager,
            NodeManager nodeManager,
            StorageManagerConfig config,
            BlockEncodingSerde blockEncodingSerde,
            MetadataDao metadataDao)
    {
        this.shardManager = shardManager;
        this.nodeId = requireNonNull(nodeManager.getCurrentNode().getNodeIdentifier(), "nodeId is null");
        this.metadataDao = metadataDao;

        checkArgument(config.getMaxShardRows() > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(config.getMaxShardRows(), MAX_ROWS);
        this.maxShardSize = requireNonNull(config.getMaxShardSize(), "maxShardSize is null");
        this.stagingDirectory = new File(config.getStagingDirectory());
        assert this.stagingDirectory.exists() && this.stagingDirectory.isDirectory() : "staging work directory not exist";
        this.dataDirectory = new File(config.getDataDirectory());
        assert this.dataDirectory.exists() && this.dataDirectory.isDirectory();
        this.pagesSerdeFactory = new PagesSerdeFactory(blockEncodingSerde, true, true);
        this.bucketPageBuffers = new ConcurrentHashMap<>();
        this.pageBuffers = new ConcurrentHashMap<>();
    }

    public boolean hasShard(UUID shardUuid)
    {
        return pageBuffers.get(shardUuid) != null;
    }

    //@Override
    public ConnectorPageSource getPageSource(UUID shardUuid, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles,
            TupleDomain<CStoreColumnHandle> predicate, RowExpression filter, OptionalLong transactionId)
    {
        MemoryShardAccessor pageBuffer = pageBuffers.get(shardUuid);
        Iterator<Page> pageIterator = ImmutableList.copyOf(pageBuffer.getPages()).iterator();
        return new MemoryPageSource(pageIterator, pageBuffer, columnHandles);
    }

    public boolean hasMemoryShardAccessor(long tableId, OptionalInt day, long transactionId, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, List<Long> sortFields, List<SortOrder> sortOrders, boolean checkSpace)
    {
        List<Object> key = ImmutableList.of(OptionalLong.of(tableId), day, bucketNumber);
        MemoryShardAccessor memoryShardAccessor = bucketPageBuffers.get(key);
        return memoryShardAccessor != null;
    }

    public MemoryShardAccessor createMemoryShardAccessor(long tableId, OptionalInt day, long transactionId, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, List<Long> sortFields, List<SortOrder> sortOrders, boolean checkSpace)
    {
        List<Object> key = ImmutableList.of(OptionalLong.of(tableId), day, bucketNumber);
        MemoryShardAccessor memoryShardAccessor = bucketPageBuffers.get(key);
        boolean newShard = memoryShardAccessor == null;
        if (newShard) {
            UUID shardUuid = UUID.randomUUID();
            shardManager.recordCreatedShard(transactionId, shardUuid);
            if (sortFields.isEmpty()) {
                memoryShardAccessor = new MemoryShardSimpleAccessor(shardUuid, maxShardSize.toBytes(), columnHandles, OptionalLong.of(tableId), day, bucketNumber);
            }
            else {
                memoryShardAccessor = new MemoryShardSortAccessor(shardUuid, maxShardSize.toBytes(), columnHandles, sortFields, OptionalLong.of(tableId), day, bucketNumber);
            }
            memoryShardAccessor = new MemoryShardWalAccessor(Paths.get(stagingDirectory.getAbsolutePath(), shardUuid.toString()).toUri(), pagesSerdeFactory, memoryShardAccessor, true);
            pageBuffers.put(shardUuid, memoryShardAccessor);
            bucketPageBuffers.put(key, memoryShardAccessor);
        }
        return memoryShardAccessor;
    }

    //@Override
    public void deleteShard(UUID shardUuid)
    {
        MemoryShardAccessor pageBuffer = pageBuffers.remove(shardUuid);
        if (pageBuffer != null) {
            Object key = ImmutableList.of(pageBuffer.getTableId(), pageBuffer.getPartitionDay(),
                    pageBuffer.getBucketNumber());
            bucketPageBuffers.remove(key);
            pageBuffer.reset();
        }
    }

    //@Override
    //@PostConstruct
    public void setup()
            throws IOException
    {
        Set<ShardMetadata> shardMetadataSet = shardManager.getNodeShards(nodeId);
        shardMetadataSet = shardMetadataSet.stream()
                .filter(ShardMetadata::isMutable)
                .collect(Collectors.toSet());
        for (ShardMetadata shardMetadata : shardMetadataSet) {
            MemoryShardAccessor CStoreShardSink = recoverMemoryPageBuffer(shardMetadata);
            pageBuffers.put(CStoreShardSink.getUuid(), CStoreShardSink);
            Object bucketKey = ImmutableList.of(CStoreShardSink.getTableId(), CStoreShardSink.getPartitionDay(), CStoreShardSink.getBucketNumber());
            bucketPageBuffers.put(bucketKey, CStoreShardSink);
        }
    }

    private MemoryShardAccessor recoverMemoryPageBuffer(ShardMetadata shardMetadata)
    {
        URI uri = Paths.get(stagingDirectory.getAbsolutePath(), shardMetadata.getShardUuid().toString()).toUri();
        return MemoryShardWalAccessor.recoverFromUri(uri, maxShardSize.toBytes(), pagesSerdeFactory);
    }
}
