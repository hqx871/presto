package github.cstore.column;

import com.facebook.presto.common.type.Type;
import github.cstore.coder.BufferCoder;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public final class ShortColumnZipReader
        extends AbstractColumnZipReader
{
    public ShortColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            Type type,
            boolean nullable)
    {
        super(rowCount, chunks, decompressor, pageSize, type, nullable);
    }

    public static Builder newBuilder(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, Type type, boolean nullable)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageSize, chunks, decompressor, type, nullable);
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        return new ShortCursor(new int[size]);
    }

    @Override
    protected PageReader nextPageReader(int offset, int end, ByteBuffer buffer, int pageNum)
    {
        return new ShortPageReader(offset, end, buffer, pageNum);
    }

    @Override
    protected NullablePageReader nextNullablePageReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer, int pageNum)
    {
        return new ShortNullablePageReader(offset, end, rawBuffer, nullBuffer, pageNum);
    }

    @Override
    protected int getValueSize()
    {
        return Short.BYTES;
    }

    private static final class ShortPageReader
            extends PageReader
    {
        private final ShortBuffer page;

        private ShortPageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.page = rawBuffer.asShortBuffer();
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
                dst.writeShort(dstStart + i, page.get(position));
            }
            return size;
        }

        @Override
        public int read(int offset, int size, VectorCursor dst, int dstOffset)
        {
            int position = offset - this.offset;
            for (int i = 0; i < size; i++) {
                dst.writeShort(i + dstOffset, page.get(position));
                position++;
            }
            return size;
        }

        public int readInt(int position)
        {
            return page.get(position - offset);
        }
    }

    private static final class ShortNullablePageReader
            extends NullablePageReader
    {
        private final ShortBuffer page;

        private ShortNullablePageReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, nullBuffer, pageNum);
            this.page = rawBuffer.asShortBuffer();
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
                    dst.writeShort(dstStart + i, page.get(position));
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
                    dst.writeShort(i + dstOffset, page.get(position));
                }
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
        private final Type type;
        private final boolean nullable;

        public Builder(int rowCount, int pageSize, BinaryOffsetVector<ByteBuffer> chunks, Decompressor decompressor, Type type, boolean nullable)
        {
            this.rowCount = rowCount;
            this.pageSize = pageSize;
            this.chunks = chunks;
            this.decompressor = decompressor;
            this.type = type;
            this.nullable = nullable;
        }

        @Override
        public CStoreColumnReader build()
        {
            return new ShortColumnZipReader(rowCount, pageSize, chunks.duplicate(), decompressor, type, nullable);
        }
    }
}
