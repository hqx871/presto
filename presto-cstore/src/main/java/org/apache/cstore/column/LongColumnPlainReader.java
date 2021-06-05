package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public final class LongColumnPlainReader
        implements CStoreColumnReader
{
    private final LongBuffer buffer;
    private final int rowCount;

    public LongColumnPlainReader(LongBuffer buffer)
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
        long[] values = new long[size];
        return new LongCursor(values);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeLong(i, buffer.get(positions[start]));
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeLong(i, buffer.get(start));
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
            dst.writeLong(buffer.get(positions[start])).closeEntry();
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
            dst.writeLong(buffer.get(start)).closeEntry();
            start++;
        }
        return size;
    }

    @Override
    public void close()
    {
    }

    public LongBuffer getDataBuffer()
    {
        return buffer;
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
        public LongColumnPlainReader duplicate()
        {
            return new LongColumnPlainReader(buffer.duplicate().asLongBuffer());
        }
    }
}
