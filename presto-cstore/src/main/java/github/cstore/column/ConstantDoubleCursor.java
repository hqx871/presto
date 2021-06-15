package github.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;

import java.util.Arrays;
import java.util.Optional;

@Deprecated
public class ConstantDoubleCursor
        implements VectorCursor
{
    protected final double doubleValue;
    private final int count;
    private final int sizeInBytes;

    public ConstantDoubleCursor(double doubleValue, int count)
    {
        this.doubleValue = doubleValue;
        this.count = count;
        this.sizeInBytes = Double.BYTES * count;
    }

    @Override
    public void clear()
    {
    }

    @Override
    public int getNullValueCount()
    {
        return 0;
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int getCapacity()
    {
        return count;
    }

    @Override
    @Deprecated
    public Block toBlock(int size)
    {
        long[] values = new long[size];
        Arrays.fill(values, Double.doubleToLongBits(doubleValue));
        return new LongArrayBlock(size, Optional.empty(), values);
    }

    @Override
    public double readDouble(int position)
    {
        return doubleValue;
    }
}
