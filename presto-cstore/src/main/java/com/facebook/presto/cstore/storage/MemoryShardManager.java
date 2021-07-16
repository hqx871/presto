package com.facebook.presto.cstore.storage;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.metadata.MetadataDao;
import com.facebook.presto.cstore.metadata.ShardManager;
import com.facebook.presto.cstore.metadata.ShardMetadata;
import com.facebook.presto.cstore.storage.organization.WalOrganizer;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    private final ConcurrentMap<UUID, MemoryShardAccessor> memoryShardMap;
    private final MetadataDao metadataDao;
    private final WriteAheadLogManager writeAheadLogManager;
    private final WalOrganizer walOrganizer;

    @Inject
    public MemoryShardManager(
            ShardManager shardManager,
            NodeManager nodeManager,
            StorageManagerConfig config,
            BlockEncodingSerde blockEncodingSerde,
            MetadataDao metadataDao,
            WriteAheadLogManager writeAheadLogManager,
            WalOrganizer walOrganizer)
    {
        this.shardManager = shardManager;
        this.nodeId = requireNonNull(nodeManager.getCurrentNode().getNodeIdentifier(), "nodeId is null");
        this.metadataDao = metadataDao;
        this.writeAheadLogManager = writeAheadLogManager;
        this.walOrganizer = walOrganizer;

        checkArgument(config.getMaxShardRows() > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(config.getMaxShardRows(), MAX_ROWS);
        this.maxShardSize = requireNonNull(config.getMaxShardSize(), "maxShardSize is null");
        this.stagingDirectory = new File(config.getStagingDirectory());
        assert this.stagingDirectory.exists() && this.stagingDirectory.isDirectory() : "staging work directory not exist";
        this.dataDirectory = new File(config.getDataDirectory());
        assert this.dataDirectory.exists() && this.dataDirectory.isDirectory();
        this.pagesSerdeFactory = new PagesSerdeFactory(blockEncodingSerde, true, true);
        this.memoryShardMap = new ConcurrentHashMap<>();
    }

    public boolean hasShard(UUID shardUuid)
    {
        return memoryShardMap.get(shardUuid) != null;
    }

    //@Override
    public ConnectorPageSource getPageSource(UUID shardUuid, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles,
            TupleDomain<CStoreColumnHandle> predicate, RowExpression filter, OptionalLong transactionId)
    {
        MemoryShardAccessor pageBuffer = memoryShardMap.get(shardUuid);
        Iterator<Page> pageIterator = ImmutableList.copyOf(pageBuffer.getPages()).iterator();
        return new MemoryPageSource(pageIterator, pageBuffer, columnHandles);
    }

    public MemoryShardAccessor createMemoryShard(long transactionId, long tableId, OptionalInt day, OptionalInt bucketNumber, List<CStoreColumnHandle> columnHandles, List<Long> sortFields, List<SortOrder> sortOrders)
    {
        MemoryShardAccessor memoryShardAccessor;
        UUID shardUuid = UUID.randomUUID();
        if (sortFields.isEmpty()) {
            memoryShardAccessor = new MemoryShardSimpleAccessor(transactionId, shardUuid, maxShardSize.toBytes(), columnHandles, OptionalLong.of(tableId), day, bucketNumber);
        }
        else {
            memoryShardAccessor = new MemoryShardSortAccessor(shardUuid, maxShardSize.toBytes(), columnHandles, sortFields, sortOrders, transactionId, OptionalLong.of(tableId), day, bucketNumber);
        }
        WriteAheadLogAppender walAppender = writeAheadLogManager.getScrollAppender(tableId, transactionId);
        memoryShardAccessor = new MemoryShardWalAccessor(shardUuid, walAppender, pagesSerdeFactory, memoryShardAccessor, transactionId);
        memoryShardMap.put(shardUuid, memoryShardAccessor);
        shardManager.recordCreatedShard(transactionId, shardUuid);
        return memoryShardAccessor;
    }

    //@Override
    public void deleteShard(UUID shardUuid)
    {
        memoryShardMap.remove(shardUuid);
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
        Map<UUID, ShardMetadata> shardMetadataMap = Maps.uniqueIndex(shardMetadataSet, ShardMetadata::getShardUuid);

        Iterable<WriteAheadLogReader> walReaders = writeAheadLogManager.listReader();
        Map<UUID, MemoryShardAccessor> shardAccessorMap = new HashMap<>();
        PagesSerde pagesSerde = pagesSerdeFactory.createPagesSerde();
        for (WriteAheadLogReader walReader : walReaders) {
            while (walReader.hasNext()) {
                WriteAheadLogRecord logRecord = walReader.next();
                UUID shardUuid = logRecord.getShardUuid();
                ShardMetadata shardMetadata = shardMetadataMap.get(shardUuid);
                if (shardMetadata == null || !shardMetadata.isMutable()) {
                    continue;
                }
                ActionType actionType = logRecord.getActionType();
                byte[] argument = logRecord.getArgument();
                switch (actionType) {
                    case CREATE: {
                        SliceInput sliceInput = new InputStreamSliceInput(new ByteArrayInputStream(argument));
                        MemoryShardAccessor memoryShardAccessor = recoverFromUri(logRecord.getTransactionId(), sliceInput, maxShardSize.toBytes());
                        shardAccessorMap.put(shardUuid, memoryShardAccessor);
                        break;
                    }
                    case APPEND: {
                        MemoryShardAccessor shardAccessor = shardAccessorMap.get(shardUuid);
                        SliceInput sliceInput = new InputStreamSliceInput(new ByteArrayInputStream(argument));
                        Iterator<Page> iterator = PagesSerdeUtil.readPages(pagesSerde, sliceInput);
                        while (iterator.hasNext()) {
                            Page page = iterator.next();
                            shardAccessor.appendPage(page);
                        }
                        break;
                    }
                    case COMMIT: {
                        MemoryShardAccessor shardAccessor = shardAccessorMap.get(shardUuid);
                        memoryShardMap.put(shardUuid, shardAccessor);
                        break;
                    }
                    case DROP:
                    case ROLLBACK: {
                        shardAccessorMap.remove(shardUuid);
                        memoryShardMap.remove(shardUuid);
                        break;
                    }
                    default:
                }
            }
        }
    }

    private MemoryShardAccessor recoverFromUri(long transactionId, SliceInput sliceInput, long maxShardSize)
    {
        long uuidMostBits = sliceInput.readLong();
        long uuidLeastBits = sliceInput.readLong();
        UUID uuid = new UUID(uuidMostBits, uuidLeastBits);
        boolean unknownTable = sliceInput.readByte() == 0;
        OptionalLong tableId = unknownTable ? OptionalLong.empty() : OptionalLong.of(sliceInput.readLong());
        boolean unknownPartitionDay = sliceInput.readByte() == 0;
        OptionalInt day = unknownPartitionDay ? OptionalInt.empty() : OptionalInt.of(sliceInput.readInt());
        boolean unknownBucketNumber = sliceInput.readByte() == 0;
        OptionalInt bucketNumber = unknownBucketNumber ? OptionalInt.empty() : OptionalInt.of(sliceInput.readInt());
        int columnCount = sliceInput.readInt();
        JsonCodec<CStoreColumnHandle> codec = JsonCodec.jsonCodec(CStoreColumnHandle.class);
        List<CStoreColumnHandle> columnHandles = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            int size = sliceInput.readInt();
            byte[] bytes = new byte[size];
            sliceInput.read(bytes);
            CStoreColumnHandle columnHandle = codec.fromBytes(bytes);
            columnHandles.add(columnHandle);
        }
        List<Long> sortColumns = columnHandles.stream()
                .filter(columnHandle -> columnHandle.getSortOrdinal().isPresent())
                .map(CStoreColumnHandle::getColumnId)
                .collect(Collectors.toList());

        if (sortColumns.isEmpty()) {
            return new MemoryShardSimpleAccessor(transactionId, uuid, maxShardSize, columnHandles, tableId, day, bucketNumber);
        }
        else {
            return new MemoryShardSortAccessor(uuid, maxShardSize, columnHandles, sortColumns, Collections.emptyList(), transactionId, tableId, day, bucketNumber);
        }
    }

    public List<MemoryShardAccessor> getShardBeforeTransaction(long transactionId)
    {
        return memoryShardMap.values().stream()
                .filter(memoryShard -> memoryShard.getTransactionId() <= transactionId)
                .sorted(Comparator.comparingLong(ShardSink::getTransactionId))
                .collect(Collectors.toList());
    }

    public List<MemoryShardAccessor> getTableShards(long tableId)
    {
        return memoryShardMap.values().stream()
                .filter(memoryShard -> memoryShard.getTableId().isPresent() && memoryShard.getTableId().getAsLong() == tableId)
                .sorted(Comparator.comparingLong(ShardSink::getTransactionId))
                .collect(Collectors.toList());
    }
}
