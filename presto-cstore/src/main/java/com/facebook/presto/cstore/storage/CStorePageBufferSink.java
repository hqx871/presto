package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.cstore.metadata.ShardInfo;
import com.facebook.presto.cstore.metadata.ShardRecorder;
import com.facebook.presto.spi.ConnectorPageSink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.util.Collection;
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
    private final OptionalInt bucketNumber;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final ShardRecorder shardRecorder;
    private final String nodeId;
    private final ConnectorPageSink delegate;
    private final long maxMemoryBytes;

    private boolean committed;
    private final MemoryShardAccessor memoryShard;
    private final boolean dirtyShard;

    public CStorePageBufferSink(
            long transactionId,
            OptionalInt bucketNumber,
            long maxShardRows,
            DataSize maxShardSize,
            ShardRecorder shardRecorder,
            String nodeId,
            ConnectorPageSink delegate,
            long maxMemoryBytes,
            MemoryShardAccessor memoryShard,
            boolean dirtyShard)
    {
        this.transactionId = transactionId;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.maxShardRows = maxShardRows;
        this.maxShardSize = maxShardSize;
        this.shardRecorder = shardRecorder;
        this.nodeId = nodeId;
        this.delegate = delegate;
        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryShard = memoryShard;
        this.dirtyShard = dirtyShard;
    }

    private void flush()
    {
        //todo flush memory and delegate?
    }

    private ShardInfo createShardInfo(UUID shardUuid, OptionalInt bucketNumber, long rowCount, long uncompressedSize)
    {
        Set<String> nodes = ImmutableSet.of(nodeId);
        return new ShardInfo(shardUuid, bucketNumber, nodes, ImmutableList.of(), rowCount, getUsedMemoryBytes(), uncompressedSize, 0, true);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (!memoryShard.canAddRows(page.getPositionCount())) {
            for (Page shardPage : memoryShard.getPages()) {
                delegate.appendPage(shardPage);
            }
            memoryShard.reset();
        }
        memoryShard.appendPage(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        checkState(!committed, "already committed");
        committed = true;
        flush();
        return delegate.finish().thenApply(fileShards -> {
            ImmutableList.Builder<Slice> builder = ImmutableList.builder();
            if (dirtyShard) {
                shardRecorder.recordCreatedShard(transactionId, memoryShard.getUuid());
                ShardInfo memoryShard = createShardInfo(this.memoryShard.getUuid(), bucketNumber,
                        this.memoryShard.getRowCount(), this.memoryShard.getUsedMemoryBytes());
                builder.add(Slices.wrappedBuffer(ShardInfo.JSON_CODEC.toJsonBytes(memoryShard)));
            }
            builder.addAll(fileShards);
            return builder.build();
        });
    }

    @Override
    public void abort()
    {
        delegate.abort();
        memoryShard.reset();
    }

    public long getUsedMemoryBytes()
    {
        return memoryShard == null ? 0 : memoryShard.getUsedMemoryBytes();
    }
}
