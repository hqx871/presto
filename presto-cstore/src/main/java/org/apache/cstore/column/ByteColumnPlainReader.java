package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.ByteBuffer;

public final class ByteColumnPlainReader
        implements CStoreColumnReader, IntVector
{
    private final ByteBuffer buffer;
    private final int rowCount;

    public ByteColumnPlainReader(ByteBuffer buffer)
    {
        this.buffer = buffer;
        this.rowCount = buffer.limit();
    }

    public static Builder builder(ByteBuffer buffer)
    {
        return new Builder(buffer);
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
        return new ByteCursor(new int[size]);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeByte(i, buffer.get(positions[start]));
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeByte(i, buffer.get(start));
            start++;
        }
        return size;
    }

    @Override
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = size + offset;
        while (start < end) {
            dst.writeByte(buffer.get(positions[start])).closeEntry();
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = size + offset;
        while (start < end) {
            dst.writeByte(buffer.get(start)).closeEntry();
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

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final ByteBuffer buffer;

        public Builder(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public ByteColumnPlainReader build()
        {
            return new ByteColumnPlainReader(buffer.duplicate());
        }
    }
}
