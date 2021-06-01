package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.DoubleBuffer;

public class DoubleColumnarReader
        implements CStoreColumnReader
{
    private final DoubleBuffer buffer;

    public DoubleColumnarReader(DoubleBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public void setup()
    {
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = start + size;
        while (start < end) {
            dst.writeLong(Double.doubleToLongBits(buffer.get(positions[start])));
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = start + size;
        while (start < end) {
            dst.writeLong(Double.doubleToLongBits(buffer.get(start)));
            start++;
        }
        return size;
    }

    @Override
    public void close()
    {
    }
}
