package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;

public final class ByteCursor
        extends AbstractVectorCursor
{
    private final int[] values;
    private final int sizeInBytes;

    public ByteCursor(int[] values)
    {
        super(values.length);
        this.values = values;
        this.sizeInBytes = Integer.BYTES * values.length + getMaskSizeInBytes();
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
    public Block toBlock(int size)
    {
        return new IntArrayBlock(size, getNullMask(), values);
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
