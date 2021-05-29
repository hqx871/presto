package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.IntBuffer;

public class IntColumnReader
        implements CStoreColumnReader, IntVector
{
    private final IntBuffer buffer;

    public IntColumnReader(IntBuffer buffer)
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
            dst.writeInt(buffer.get(positions[i + offset]));
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        for (int i = 0; i < size; i++) {
            dst.writeInt(buffer.get(i + offset));
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
