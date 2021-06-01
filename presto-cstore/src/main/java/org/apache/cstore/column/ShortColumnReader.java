package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.ShortBuffer;

public class ShortColumnReader
        implements CStoreColumnReader, IntVector
{
    private final ShortBuffer buffer;

    public ShortColumnReader(ShortBuffer buffer)
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
            dst.writeShort(buffer.get(positions[start]));
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
            dst.writeShort(buffer.get(start));
            start++;
        }
        return size;
    }

    @Override
    public void close()
    {
    }

    @Override
    public int readInt(int position)
    {
        return buffer.get(position);
    }
}
