package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.IntArrayBlock;

import java.nio.IntBuffer;
import java.util.Optional;

public final class IntColumnReader
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
    public VectorCursor createVectorCursor(int size)
    {
        return new Cursor(new int[size]);
    }

    @Override
    public int read(int[] positions, int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeInt(i, buffer.get(positions[start]));
            start++;
        }
        return size;
    }

    @Override
    public int read(int offset, int size, VectorCursor dst)
    {
        int start = offset;
        for (int i = 0; i < size; i++) {
            dst.writeInt(i, buffer.get(start));
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
            dst.writeInt(buffer.get(positions[start]));
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
            dst.writeInt(buffer.get(start));
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

    private static final class Cursor
            implements VectorCursor
    {
        private final int[] values;
        private final int sizeInBytes;

        private Cursor(int[] values)
        {
            this.values = values;
            this.sizeInBytes = getCapacity() * Integer.BYTES;
        }

        @Override
        public void writeByte(int position, byte value)
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
            return new IntArrayBlock(size, Optional.empty(), values);
        }
    }
}
