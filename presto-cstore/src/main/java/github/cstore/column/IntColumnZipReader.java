package github.cstore.column;

import com.facebook.presto.common.type.Type;
import github.cstore.coder.BufferCoder;
import io.airlift.compress.Decompressor;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public final class IntColumnZipReader
        extends AbstractColumnZipReader
{
    public IntColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            Type type,
            boolean nullable)
    {
        super(rowCount, chunks, decompressor, pageSize, type, nullable);
    }

    public static IntColumnZipReader.Builder newBuilder(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, Type type, boolean nullable)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageSize, chunks, decompressor, type, nullable);
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        return new IntCursor(new int[size]);
    }

    @Override
    protected PageReader nextPageReader(int offset, int end, ByteBuffer buffer, int pageNum)
    {
        return new IntPageReader(offset, end, buffer, pageNum);
    }

    @Override
    protected NullablePageReader nextNullablePageReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer, int pageNum)
    {
        return new IntNullablePageReader(offset, end, rawBuffer, nullBuffer, pageNum);
    }

    @Override
    protected int getValueSize()
    {
        return Integer.BYTES;
    }

    private static final class IntPageReader
            extends PageReader
    {
        private final IntBuffer page;

        private IntPageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.page = rawBuffer.asIntBuffer();
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
                dst.writeInt(dstStart + i, page.get(position));
            }
            return size;
        }

        @Override
        public int read(int offset, int size, VectorCursor dst, int dstOffset)
        {
            int position = offset - this.offset;
            for (int i = 0; i < size; i++) {
                dst.writeInt(i + dstOffset, page.get(position));
                position++;
            }
            return size;
        }

        public int readInt(int position)
        {
            return page.get(position - offset);
        }
    }

    private static final class IntNullablePageReader
            extends NullablePageReader
    {
        private final IntBuffer page;

        private IntNullablePageReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, nullBuffer, pageNum);
            this.page = rawBuffer.asIntBuffer();
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
                    dst.writeInt(dstStart + i, page.get(position));
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
                    dst.writeInt(i + dstOffset, page.get(position));
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
        public IntColumnZipReader build()
        {
            return new IntColumnZipReader(rowCount, pageSize, chunks.duplicate(), decompressor, type, nullable);
        }
    }
}
