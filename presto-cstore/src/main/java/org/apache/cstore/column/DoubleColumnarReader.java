package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlock;

import java.nio.DoubleBuffer;
import java.util.Optional;

public final class DoubleColumnarReader
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
        int start = offset;
        int end = start + size;
        while (start < end) {
            dst.writeLong(Double.doubleToLongBits(buffer.get(positions[start])));
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
            dst.writeLong(Double.doubleToLongBits(buffer.get(start)));
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
        return new Cursor(values);
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

    private static final class Cursor
            implements VectorCursor
    {
        private final long[] values;
        private final int sizeInBytes;

        private Cursor(long[] values)
        {
            this.values = values;
            this.sizeInBytes = getCapacity() * Double.BYTES;
        }

        @Override
        public void writeDouble(int position, double value)
        {
            values[position] = Double.doubleToLongBits(value);
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
