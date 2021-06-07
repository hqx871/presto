package org.apache.cstore.column;

import com.facebook.presto.common.type.TinyintType;
import io.airlift.compress.Decompressor;
import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;

public final class ByteColumnZipReader
        extends AbstractColumnZipReader
{
    public ByteColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            TinyintType type)
    {
        super(rowCount, chunks, decompressor, pageSize, type);
    }

    public static ByteColumnZipReader decode(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, TinyintType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new ByteColumnZipReader(rowCount, pageSize, chunks, decompressor, type);
    }

    public static Builder newBuilder(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, TinyintType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageSize, chunks, decompressor, type);
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        int[] values = new int[size];
        return new ByteCursor(values);
    }

    @Override
    protected PageReader nextPageReader(int offset, int end, ByteBuffer buffer, int pageNum)
    {
        return new BytePageReader(offset, end, buffer, pageNum);
    }

    @Override
    protected int getValueSize()
    {
        return Byte.BYTES;
    }

    private static final class BytePageReader
            extends PageReader
    {
        private final ByteBuffer page;

        private BytePageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.page = rawBuffer;
        }

        @Override
        public int read(int[] positions, int offset, int size, VectorCursor dst, int dstStart)
        {
            for (int i = 0; i < size; i++) {
                int position = positions[i + offset];
                if (position >= end) {
                    return i;
                }
                position -= this.offset;
                dst.writeByte(dstStart + i, page.get(position));
            }
            return size;
        }

        @Override
        public int read(int offset, int size, VectorCursor dst, int dstOffset)
        {
            int position = offset - this.offset;
            for (int i = 0; i < size; i++) {
                dst.writeByte(i + dstOffset, page.get(position));
                position++;
            }
            return size;
        }

        public int readInt(int position)
        {
            return page.get(position - offset);
        }
    }

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final int rowCount;
        private final int pageSize;
        private final BinaryOffsetVector<ByteBuffer> chunks;
        private final Decompressor decompressor;
        private final TinyintType type;

        public Builder(int rowCount, int pageSize, BinaryOffsetVector<ByteBuffer> chunks, Decompressor decompressor, TinyintType type)
        {
            this.rowCount = rowCount;
            this.pageSize = pageSize;
            this.chunks = chunks;
            this.decompressor = decompressor;
            this.type = type;
        }

        @Override
        public CStoreColumnReader build()
        {
            return new ByteColumnZipReader(rowCount, pageSize, chunks.duplicate(), decompressor, type);
        }
    }
}
