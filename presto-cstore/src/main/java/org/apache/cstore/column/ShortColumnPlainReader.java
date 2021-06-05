package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.ShortBuffer;

public final class ShortColumnPlainReader
        implements CStoreColumnReader, IntVector
{
    private final ShortBuffer buffer;
    private final int rowCount;

    public ShortColumnPlainReader(ShortBuffer buffer)
    {
        this.buffer = buffer;
        this.rowCount = buffer.limit();
    }

    @Override
    public void setup()
    {
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        return new ShortCursor(new int[size]);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeShort(i, buffer.get(positions[start]));
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeShort(i, buffer.get(start));
            start++;
        }
        return size;
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = start + size;
        while (start < end) {
            dst.writeShort(buffer.get(positions[start])).closeEntry();
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
            dst.writeShort(buffer.get(start)).closeEntry();
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
