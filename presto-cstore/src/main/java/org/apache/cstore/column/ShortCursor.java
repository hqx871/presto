package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;

import java.util.Optional;

public class ShortCursor
        implements VectorCursor
{
    private final int[] values;
    private final int sizeInBytes;

    public ShortCursor(int[] values)
    {
        this.values = values;
        this.sizeInBytes = getCapacity() * Integer.BYTES;
    }

    @Override
    public void writeShort(int position, short value)
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
