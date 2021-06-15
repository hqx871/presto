package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;

import java.util.Optional;

public final class LongCursor
        extends AbstractVectorCursor
{
    private final long[] values;
    private final int sizeInBytes;

    public LongCursor(long[] values)
    {
        super(values.length);
        this.values = values;
        this.sizeInBytes = getCapacity() * Long.BYTES + getMaskSizeInBytes();
    }

    @Override
    public void writeInt(int position, int value)
    {
        values[position] = value;
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
    public Block toBlock(int size)
    {
        return new LongArrayBlock(size, Optional.empty(), values);
    }

    @Override
    public long readLong(int position)
    {
        return values[0];
    }
}
