package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ByteArrayBlock;

import java.util.Optional;

public class BooleanCursor
        implements VectorCursor
{
    private final byte[] values;
    private final int sizeInBytes;

    public BooleanCursor(byte[] values)
    {
        this.values = values;
        this.sizeInBytes = Byte.BYTES * getCapacity();
    }

    @Override
    public void writeBoolean(int position, boolean value)
    {
        values[position] = (byte) (value ? 1 : 0);
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
        return new ByteArrayBlock(size, Optional.empty(), values);
    }

    @Override
    public byte readByte(int position)
    {
        return values[position];
    }
}
