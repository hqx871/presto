package github.cstore.column;

import com.facebook.presto.common.type.Type;
import github.cstore.coder.BufferCoder;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public final class LongColumnZipReader
        extends AbstractColumnZipReader
{
    public LongColumnZipReader(int rowCount,
            int pageRowCount,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            Type type,
            boolean nullable)
    {
        super(rowCount, chunks, decompressor, pageRowCount, type, nullable);
    }

    public static Builder newBuilder(int rowCount, int pageRowCount, ByteBuffer buffer, Decompressor decompressor, Type type, boolean nullable)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageRowCount, chunks, decompressor, type, nullable);
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
    protected NullablePageReader nextNullablePageReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer, int pageNum)
    {
        return new LongNullablePageReader(offset, end, rawBuffer, nullBuffer, pageNum);
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
        public int read(int[] positions, int offset, int size, VectorCursor dst, int dstStart)
        {
            for (int i = 0; i < size; i++) {
                int position = positions[i + offset];
                if (position >= end) {
                    return i;
                }
                position -= this.offset;
                dst.writeLong(dstStart + i, page.get(position));
            }
            return size;
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

    private static final class LongNullablePageReader
            extends NullablePageReader
    {
        private final LongBuffer page;

        private LongNullablePageReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, nullBuffer, pageNum);
            this.page = rawBuffer.asLongBuffer();
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
                if (isNull(position)) {
                    dst.setNull(dstStart + i);
                }
                else {
                    dst.writeLong(dstStart + i, page.get(position));
                }
            }
            return size;
        }

        @Override
        public int read(int offset, int size, VectorCursor dst, int dstOffset)
        {
            int position = offset - this.offset;
            for (int i = 0; i < size; i++) {
                if (isNull(position)) {
                    dst.setNull(i + dstOffset);
                }
                else {
                    dst.writeLong(i + dstOffset, page.get(position));
                }

                position++;
            }
            return size;
        }
    }

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final int rowCount;
        private final int pageRowCount;
        private final BinaryOffsetVector<ByteBuffer> chunks;
        private final Decompressor decompressor;
        private final Type type;
        private final boolean nullable;

        public Builder(int rowCount, int pageRowCount, BinaryOffsetVector<ByteBuffer> chunks, Decompressor decompressor, Type type, boolean nullable)
        {
            this.rowCount = rowCount;
            this.pageRowCount = pageRowCount;
            this.chunks = chunks;
            this.decompressor = decompressor;
            this.type = type;
            this.nullable = nullable;
        }

        @Override
        public LongColumnZipReader build()
        {
            return new LongColumnZipReader(rowCount, pageRowCount, chunks.duplicate(), decompressor, type, nullable);
        }
    }
}