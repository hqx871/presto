package github.cstore.column;

import com.facebook.presto.common.type.DoubleType;
import io.airlift.compress.Decompressor;
import github.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

public final class DoubleColumnZipReader
        extends AbstractColumnZipReader
{
    public DoubleColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            DoubleType type)
    {
        super(rowCount, chunks, decompressor, pageSize, type);
    }

    public static DoubleColumnZipReader decode(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, DoubleType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new DoubleColumnZipReader(rowCount, pageSize, chunks, decompressor, type);
    }

    public static Builder newBuilder(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, DoubleType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageSize, chunks, decompressor, type);
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        long[] values = new long[size];
        return new DoubleCursor(values);
    }

    @Override
    protected PageReader nextPageReader(int offset, int end, ByteBuffer buffer, int pageNum)
    {
        return new DoublePageReader(offset, end, buffer, pageNum);
    }

    @Override
    protected int getValueSize()
    {
        return Double.BYTES;
    }

    private static final class DoublePageReader
            extends PageReader
    {
        private final DoubleBuffer page;

        private DoublePageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.page = rawBuffer.asDoubleBuffer();
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
                dst.writeDouble(dstStart + i, page.get(position));
            }
            return size;
        }

        @Override
        public int read(int offset, int size, VectorCursor dst, int dstOffset)
        {
            int position = offset - this.offset;
            for (int i = 0; i < size; i++) {
                dst.writeDouble(i + dstOffset, page.get(position));
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
        private final DoubleType type;

        public Builder(int rowCount, int pageSize, BinaryOffsetVector<ByteBuffer> chunks, Decompressor decompressor, DoubleType type)
        {
            this.rowCount = rowCount;
            this.pageSize = pageSize;
            this.chunks = chunks;
            this.decompressor = decompressor;
            this.type = type;
        }

        @Override
        public DoubleColumnZipReader build()
        {
            return new DoubleColumnZipReader(rowCount, pageSize, chunks.duplicate(), decompressor, type);
        }
    }
}
