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
        for (int i = 0; i < size; i++) {
            dst.writeLong(Double.doubleToLongBits(buffer.get(positions[i + offset])));
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        for (int i = 0; i < size; i++) {
            dst.writeLong(Double.doubleToLongBits(buffer.get(i + offset)));
        }
        return size;
    }

    @Override
    public void close()
    {
    }
}
