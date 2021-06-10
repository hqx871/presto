package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;

import java.util.Optional;

public final class ShortCursor
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

    @Override
    public short readShort(int position)
    {
        return (short) values[position];
    }

    @Override
    public int readInt(int position)
    {
        return values[position];
    }

    @Override
    public long readLong(int position)
    {
        return values[position];
    }

    @Override
    public double readDouble(int position)
    {
        return values[position];
    }
}
