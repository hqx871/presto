package org.apache.cstore.column;

import com.facebook.presto.common.type.BigintType;
import io.airlift.compress.Decompressor;
import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public final class LongColumnZipReader
        extends AbstractColumnZipReader
{
    public LongColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            BigintType type)
    {
        super(rowCount, chunks, decompressor, pageSize, type);
    }

    public static LongColumnZipReader decode(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, BigintType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new LongColumnZipReader(rowCount, pageSize, chunks, decompressor, type);
    }

    public static Builder newBuilder(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, BigintType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageSize, chunks, decompressor, type);
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        long[] values = new long[size];
        return new LongCursor(values);
    }

    @Override
    protected AbstractColumnZipReader.PageReader nextPageReader(int offset, int end, ByteBuffer buffer, int pageNum)
    {
        return new LongPageReader(offset, end, buffer, pageNum);
    }

    @Override
    protected int getValueSize()
    {
        return Long.BYTES;
    }

    private static final class LongPageReader
            extends PageReader
    {
        private final LongBuffer page;

        private LongPageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.page = rawBuffer.asLongBuffer();
        }

        @Override
        public void read(int[] positions, int offset, int size, VectorCursor dst, int dstStart)
        {
            for (int i = 0; i < size; i++) {
                int position = positions[i + offset] - this.offset;
                dst.writeLong(dstStart + i, page.get(position));
            }
        }

        @Override
        public int read(int offset, int size, VectorCursor dst, int dstOffset)
        {
            int position = offset - this.offset;
            for (int i = 0; i < size; i++) {
                dst.writeLong(i + dstOffset, page.get(position));
                position++;
            }
            return size;
        }
    }

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final int rowCount;
        private final int pageSize;
        private final BinaryOffsetVector<ByteBuffer> chunks;
        private final Decompressor decompressor;
        private final BigintType type;

        public Builder(int rowCount, int pageSize, BinaryOffsetVector<ByteBuffer> chunks, Decompressor decompressor, BigintType type)
        {
            this.rowCount = rowCount;
            this.pageSize = pageSize;
            this.chunks = chunks;
            this.decompressor = decompressor;
            this.type = type;
        }

        @Override
        public LongColumnZipReader duplicate()
        {
            return new LongColumnZipReader(rowCount, pageSize, chunks.duplicate(), decompressor, type);
        }
    }
}