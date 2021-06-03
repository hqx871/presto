package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlock;

import java.nio.LongBuffer;
import java.util.Optional;

public final class LongColumnReader
        implements CStoreColumnReader
{
    private final LongBuffer buffer;
    private final int rowCount;

    public LongColumnReader(LongBuffer buffer)
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
        long[] values = new long[size];
        return new Cursor(values);
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

    private static final class Cursor
            implements VectorCursor
    {
        private final long[] values;
        private final int sizeInBytes;

        private Cursor(long[] values)
        {
            this.values = values;
            this.sizeInBytes = getCapacity() * Long.BYTES;
        }

        @Override
        public void writeLong(int position, long value)
        {
            values[position] = value;
        }

        @Override
        public int getSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public int getCapacity()
        {
            return values.length;
        }

        @Override
        public Block toBlock(int size)
        {
            return new LongArrayBlock(size, Optional.empty(), values);
        }
    }
}
