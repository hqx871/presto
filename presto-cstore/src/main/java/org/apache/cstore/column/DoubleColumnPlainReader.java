package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

public final class DoubleColumnPlainReader
        implements CStoreColumnReader
{
    private final DoubleBuffer buffer;
    private final int rowCount;

    public DoubleColumnPlainReader(DoubleBuffer buffer)
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
    public int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        int start = offset;
        int end = start + size;
        while (start < end) {
            dst.writeLong(Double.doubleToLongBits(buffer.get(positions[start]))).closeEntry();
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
            dst.writeLong(Double.doubleToLongBits(buffer.get(start))).closeEntry();
            start++;
        }
        return size;
    }

    @Override
    public void close()
    {
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        long[] values = new long[size];
        return new DoubleCursor(values);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeDouble(i, buffer.get(positions[start]));
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeDouble(i, buffer.get(start));
            start++;
        }
        return size;
    }

    public DoubleBuffer getDataBuffer()
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
        public DoubleColumnPlainReader build()
        {
            return new DoubleColumnPlainReader(buffer.duplicate().asDoubleBuffer());
        }
    }
}
