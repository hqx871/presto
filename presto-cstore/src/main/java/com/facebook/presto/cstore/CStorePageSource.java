package com.facebook.presto.cstore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.relation.RowExpression;
import org.apache.cstore.CStoreReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

public class CStorePageSource
        implements ConnectorPageSource
{
    private List<Block> blocks;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private long systemMemoryUsage;
    private final CStoreReader reader;

    public CStorePageSource(CStoreSplit split, CStoreColumnHandle[] columnHandles, @Nullable RowExpression filter)
    {
        this.reader = new CStoreReader(split, columnHandles, filter, 1024);
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
        return reader.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return reader.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public void close()
            throws IOException
    {
        reader.close();
    }
}
