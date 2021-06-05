package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;

import java.util.Optional;

public class IntCursor
        implements VectorCursor
{
    protected final int[] values;
    private final int sizeInBytes;

    IntCursor(int[] values)
    {
        this.values = values;
        this.sizeInBytes = getCapacity() * Integer.BYTES;
    }

    @Override
    public final void writeByte(int position, byte value)
    {
        values[position] = value;
    }

    public final void writeInt(int position, int value)
    {
        values[position] = value;
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public final int getCapacity()
    {
        return values.length;
    }

    @Override
    public Block toBlock(int size)
    {
        return new IntArrayBlock(size, Optional.empty(), values);
    }
}
