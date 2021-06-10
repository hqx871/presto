package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;

import java.util.Optional;

public final class ByteCursor
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

    @Override
    public byte readByte(int position)
    {
        return (byte) values[position];
    }

    @Override
    public short readShort(int position)
    {
        return (byte) values[position];
    }

    @Override
    public int readInt(int position)
    {
        return (byte) values[position];
    }

    @Override
    public long readLong(int position)
    {
        return (byte) values[position];
    }

    @Override
    public double readDouble(int position)
    {
        return (byte) values[position];
    }
}
