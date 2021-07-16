package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.facebook.presto.spi.ConnectorPageSink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CStorePageBufferSink
        implements ConnectorPageSink
{
    private final long transactionId;
    private final ShardRecorder shardRecorder;
    private final String nodeId;
    private final ConnectorPageSink delegate;

    private boolean committed;
    private MemoryShardAccessor memoryShard;
    private final MemoryShardManager memoryShardManager;
    private final long tableId;
    private final OptionalInt day;
    private final OptionalInt bucketNumber;
    private final List<CStoreColumnHandle> columnHandles;
    private final List<Long> sortFields;
    private final List<SortOrder> sortOrders;
    private final boolean checkSpace;

    public CStorePageBufferSink(
            long transactionId,
            long tableId,
            OptionalInt day,
            OptionalInt bucketNumber,
            List<CStoreColumnHandle> columnHandles,
            List<Long> sortFields,
            List<SortOrder> sortOrders,
            boolean checkSpace,
            ShardRecorder shardRecorder,
            String nodeId,
            ConnectorPageSink delegate,
            MemoryShardManager memoryShardManager)
    {
        this.transactionId = transactionId;
        this.tableId = tableId;
        this.day = day;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.columnHandles = columnHandles;
        this.sortFields = sortFields;
        this.sortOrders = sortOrders;
        this.checkSpace = checkSpace;
        this.shardRecorder = shardRecorder;
        this.nodeId = nodeId;
        this.delegate = delegate;
        this.memoryShardManager = memoryShardManager;
    }

    private ShardInfo createShardInfo(UUID shardUuid, OptionalInt bucketNumber, long rowCount, long uncompressedSize)
    {
        Set<String> nodes = ImmutableSet.of(nodeId);
        return new ShardInfo(shardUuid, bucketNumber, nodes, ImmutableList.of(), rowCount, getUsedMemoryBytes(), uncompressedSize, 0, true);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        createMemoryShardIfNecessary(page.getPositionCount());
        memoryShard.appendPage(page);
        return NOT_BLOCKED;
    }

    private void createMemoryShardIfNecessary(int size)
    {
        if (memoryShard != null && !memoryShard.canAddRows(size)) {
            for (Page shardPage : memoryShard.getPages()) {
                delegate.appendPage(shardPage);
            }
            memoryShard.commit();
            memoryShardManager.deleteShard(memoryShard.getUuid());
            memoryShard = null;
        }
        if (memoryShard == null) {
            this.memoryShard = memoryShardManager.createMemoryShard(transactionId, tableId, day, bucketNumber, columnHandles, sortFields, sortOrders);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        checkState(!committed, "already committed");
        committed = true;
        memoryShard.commit();
        return delegate.finish().thenApply(fileShards -> {
            ImmutableList.Builder<Slice> builder = ImmutableList.builder();
            ShardInfo memoryShard = createShardInfo(this.memoryShard.getUuid(), bucketNumber,
                    this.memoryShard.getRowCount(), this.memoryShard.getUsedMemoryBytes());
            builder.addAll(fileShards);
            builder.add(Slices.wrappedBuffer(ShardInfo.JSON_CODEC.toJsonBytes(memoryShard)));
            return builder.build();
        });
    }

    @Override
    public void abort()
    {
        delegate.abort();
        memoryShard.rollback();
    }

    public long getUsedMemoryBytes()
    {
        return memoryShard == null ? 0 : memoryShard.getUsedMemoryBytes();
    }
}
