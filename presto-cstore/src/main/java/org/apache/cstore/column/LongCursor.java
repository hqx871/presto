package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;

import java.util.Optional;

public class LongCursor
        implements VectorCursor
{
    private final long[] values;
    private final int sizeInBytes;

    public LongCursor(long[] values)
    {
        this.values = values;
        this.sizeInBytes = getCapacity() * Long.BYTES;
    }

    @Override
    public void writeLong(int position, long value)
    {
        values[position] = value;
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int getCapacity()
    {
        return values.length;
    }

    @Override
    public Block toBlock(int size)
    {
        return new LongArrayBlock(size, Optional.empty(), values);
    }
}
