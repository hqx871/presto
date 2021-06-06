package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;

import java.util.Optional;

public class ByteCursor
        implements VectorCursor
{
    private final int[] values;
    private final int sizeInBytes;

    public ByteCursor(int[] values)
    {
        this.values = values;
        this.sizeInBytes = Integer.BYTES * getCapacity();
    }

    @Override
    public void writeByte(int position, byte value)
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
        return new IntArrayBlock(size, Optional.empty(), values);
    }
}
