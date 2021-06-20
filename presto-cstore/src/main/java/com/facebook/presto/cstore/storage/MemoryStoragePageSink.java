package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.backup.BackupManager;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MemoryStoragePageSink
        implements StoragePageSink
{
    private final long transactionId;
    private final List<Type> columnTypes;
    private final OptionalInt bucketNumber;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final ShardRecorder shardRecorder;
    private final StorageService storageService;
    private final BackupManager backupManager;
    private final String nodeId;
    private final ExecutorService commitExecutor;
    private final StoragePageSink delegate;
    private final long maxMemoryBytes;

    //private final List<ShardInfo> shards = new ArrayList<>();
    //private final List<CompletableFuture<?>> futures = new ArrayList<>();

    private boolean committed;
    //private UUID shardUuid;
    private final MemoryPageBuffer memoryPageBuffer;
    private final boolean dirtyShard;

    public MemoryStoragePageSink(
            long transactionId,
            List<CStoreColumnHandle> columnHandles,
            OptionalInt bucketNumber,
            long maxShardRows,
            DataSize maxShardSize,
            ShardRecorder shardRecorder,
            StorageService storageService,
            BackupManager backupManager,
            String nodeId,
            ExecutorService commitExecutor,
            StoragePageSink delegate,
            long maxMemoryBytes,
            MemoryPageBuffer memoryPageBuffer,
            boolean dirtyShard)
    {
        this.transactionId = transactionId;
        this.columnTypes = columnHandles.stream().map(CStoreColumnHandle::getColumnType).collect(toList());
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.maxShardRows = maxShardRows;
        this.maxShardSize = maxShardSize;
        this.shardRecorder = shardRecorder;
        this.storageService = storageService;
        this.backupManager = backupManager;
        this.nodeId = nodeId;
        this.commitExecutor = commitExecutor;
        this.delegate = delegate;
        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryPageBuffer = memoryPageBuffer;
        this.dirtyShard = dirtyShard;
    }

    @Override
    public void appendPages(List<Page> pages)
    {
        //createWriterIfNecessary();
        for (Page page : pages) {
            if (!memoryPageBuffer.canAddRows(page.getPositionCount())) {
                delegate.appendPages(memoryPageBuffer.getPages());
                memoryPageBuffer.reset();
                //memoryPageBuffer = null;
                //createWriterIfNecessary();
            }
            memoryPageBuffer.appendPage(page);
        }
    }

    @Override
    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        //createWriterIfNecessary();
        if (!memoryPageBuffer.canAddRows(positionIndexes.length)) {
            delegate.appendPages(memoryPageBuffer.getPages());
            memoryPageBuffer.reset();
            //createWriterIfNecessary();
        }
        memoryPageBuffer.appendPages(inputPages, pageIndexes, positionIndexes);
    }

    @Override
    public boolean isFull()
    {
        if (memoryPageBuffer == null) {
            return false;
        }
        return (memoryPageBuffer.getRowCount() >= maxShardRows) || (memoryPageBuffer.getUsedMemoryBytes() >= maxShardSize.toBytes());
    }

    @Override
    public void flush()
    {
    }

    private ShardInfo createShardInfo(UUID shardUuid, OptionalInt bucketNumber, long rowCount, long uncompressedSize)
    {
        Set<String> nodes = ImmutableSet.of(nodeId);
        return new ShardInfo(shardUuid, bucketNumber, nodes, ImmutableList.of(), rowCount, getUsedMemoryBytes(), uncompressedSize, 0, true);
    }

    @Override
    public CompletableFuture<List<ShardInfo>> commit()
    {
        checkState(!committed, "already committed");
        committed = true;
        flush();
        return delegate.commit().thenApply(list -> {
            ImmutableList.Builder<ShardInfo> builder = ImmutableList.builder();
            if (dirtyShard) {
                shardRecorder.recordCreatedShard(transactionId, memoryPageBuffer.getUuid());
                ShardInfo memoryShard = createShardInfo(memoryPageBuffer.getUuid(), bucketNumber,
                        memoryPageBuffer.getRowCount(), memoryPageBuffer.getUsedMemoryBytes());
                builder.add(memoryShard);
            }
            builder.addAll(list);
            return builder.build();
        });
    }

    @Override
    public void rollback()
    {
        delegate.rollback();
        memoryPageBuffer.reset();
    }

    public long getUsedMemoryBytes()
    {
        return memoryPageBuffer == null ? 0 : memoryPageBuffer.getUsedMemoryBytes();
    }
}
