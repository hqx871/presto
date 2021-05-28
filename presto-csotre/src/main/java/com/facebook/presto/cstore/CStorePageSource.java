package com.facebook.presto.cstore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.relation.RowExpression;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

public class CStorePageSource
        implements ConnectorPageSource
{
    private final String path;
    private final List<ColumnHandle> columnHandles;
    @Nullable
    private final RowExpression filter;
    private List<Block> blocks;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    public CStorePageSource(String path, List<ColumnHandle> columnHandles, @Nullable RowExpression filter)
    {
        this.path = path;
        this.columnHandles = columnHandles;
        this.filter = filter;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return false;
    }

    @Override
    public Page getNextPage()
    {
        return null;
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
