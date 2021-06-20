package com.facebook.presto.cstore.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.cstore.CStoreColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MemoryPageSource
        implements ConnectorPageSource
{
    private final Iterator<Page> pageIterator;
    private final Map<Long, Integer> columnIndexes;
    private final List<CStoreColumnHandle> columnHandles;

    private long completeBytes;
    private long completePositions;
    private long readTimeNanos;
    private long rowId;

    public MemoryPageSource(Iterator<Page> pageIterator,
            MemoryPageBuffer pageBuffer,
            List<CStoreColumnHandle> columnHandles)
    {
        this.pageIterator = pageIterator;
        this.columnHandles = columnHandles;
        this.columnIndexes = new HashMap<>();
        for (int i = 0; i < pageBuffer.getColumnHandles().size(); i++) {
            columnIndexes.put(columnHandles.get(i).getColumnId(), i);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completeBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completePositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return !pageIterator.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Page sourcePage = pageIterator.next();
        Block[] blocks = new Block[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            CStoreColumnHandle columnHandle = columnHandles.get(i);
            if (columnHandle.isShardRowId()) {
                BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, sourcePage.getPositionCount());
                for (int j = 0; j < sourcePage.getPositionCount(); j++) {
                    BigintType.BIGINT.writeLong(blockBuilder, rowId++);
                }
                blocks[i] = blockBuilder.build();
            }
            else {
                blocks[i] = sourcePage.getBlock(columnIndexes.get(columnHandle.getColumnId()));
            }
        }
        Page page = new Page(sourcePage.getPositionCount(), blocks);
        readTimeNanos += stopwatch.elapsed(TimeUnit.NANOSECONDS);
        completeBytes += sourcePage.getApproximateLogicalSizeInBytes();
        completePositions += sourcePage.getPositionCount();
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
