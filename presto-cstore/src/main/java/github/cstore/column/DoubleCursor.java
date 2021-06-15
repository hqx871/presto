package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;

import java.util.Optional;

public final class DoubleCursor
        extends AbstractVectorCursor
{
    private final long[] values;
    private final int sizeInBytes;

    public DoubleCursor(long[] values)
    {
        super(values.length);
        this.values = values;
        this.sizeInBytes = getCapacity() * Double.BYTES + getMaskSizeInBytes();
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
