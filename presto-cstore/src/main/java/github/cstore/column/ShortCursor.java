package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;

import java.util.Optional;

public final class ShortCursor
        extends AbstractVectorCursor
{
    private final int[] values;
    private final int sizeInBytes;

    public ShortCursor(int[] values)
    {
        super(values.length);
        this.values = values;
        this.sizeInBytes = getCapacity() * Integer.BYTES + getMaskSizeInBytes();
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
