package github.cstore.column;

import com.facebook.presto.common.type.IntegerType;
import io.airlift.compress.Decompressor;
import github.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public final class IntColumnZipReader
        extends AbstractColumnZipReader
{
    public IntColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            IntegerType type)
    {
        super(rowCount, chunks, decompressor, pageSize, type);
    }

    public static IntColumnZipReader decode(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, IntegerType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new IntColumnZipReader(rowCount, pageSize, chunks, decompressor, type);
    }

    public static Builder newBuilder(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, IntegerType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageSize, chunks, decompressor, type);
    }

    public static Builder decodeFactory(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, IntegerType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new Builder(rowCount, pageSize, chunks, decompressor, type);
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

    public static class Builder
            implements CStoreColumnReader.Builder
    {
        private final int rowCount;
        private final int pageSize;
        private final BinaryOffsetVector<ByteBuffer> chunks;
        private final Decompressor decompressor;
        private final IntegerType type;

        public Builder(int rowCount, int pageSize, BinaryOffsetVector<ByteBuffer> chunks, Decompressor decompressor, IntegerType type)
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
            return new IntColumnZipReader(rowCount, pageSize, chunks.duplicate(), decompressor, type);
        }
    }
}
