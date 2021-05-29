package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.LongBuffer;

public class LongColumnReader
        implements CStoreColumnReader
{
    private final LongBuffer buffer;

    public LongColumnReader(LongBuffer buffer)
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
            dst.writeLong(buffer.get(positions[i + offset]));
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        for (int i = 0; i < size; i++) {
            dst.writeLong(buffer.get(i + offset));
        }
        return size;
    }

    @Override
    public void close()
    {
    }
}
