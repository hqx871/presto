package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;

import java.util.Optional;

public final class DoubleCursor
        implements VectorCursor
{
    private final long[] values;
    private final int sizeInBytes;

    public DoubleCursor(long[] values)
    {
        this.values = values;
        this.sizeInBytes = getCapacity() * Double.BYTES;
    }

    @Override
    public void writeDouble(int position, double value)
    {
        values[position] = Double.doubleToLongBits(value);
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

    @Override
    public double readDouble(int position)
    {
        return values[position];
    }
}
