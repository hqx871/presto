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
        int start = offset;
        int end = start + size;
        while (start < end) {
            dst.writeLong(buffer.get(positions[start]));
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
            dst.writeLong(buffer.get(start));
            start++;
        }
        return size;
    }

    @Override
    public void close()
    {
    }
}
