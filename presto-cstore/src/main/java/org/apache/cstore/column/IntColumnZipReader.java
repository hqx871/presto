package org.apache.cstore.column;

import com.facebook.presto.common.type.IntegerType;
import io.airlift.compress.Decompressor;
import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public final class IntColumnZipReader
        extends AbstractColumnZipReader
        implements IntVector
{
    private final IntPageReader pageReader;

    public IntColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            IntegerType type)
    {
        this(rowCount, pageSize, chunks, decompressor,
                new IntPageReader(0, 0, ByteBuffer.wrap(new byte[0]), -1), type);
    }

    private IntColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            IntPageReader pageReader,
            IntegerType type)
    {
        super(rowCount, chunks, decompressor, pageSize, type, pageReader);
        this.pageReader = pageReader;
    }

    public static IntColumnZipReader decode(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, IntegerType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new IntColumnZipReader(rowCount, pageSize, chunks, decompressor, type);
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

    @Override
    public int readInt(int position)
    {
        loadPage(position);
        return pageReader.readInt(position);
    }

    private static class IntPageReader
            extends PageReader
    {
        private final IntBuffer page;

        private IntPageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.page = rawBuffer.asIntBuffer();
        }

        @Override
        public void read(int[] positions, int offset, int size, VectorCursor dst, int dstStart)
        {
            for (int i = 0; i < size; i++) {
                int position = positions[i + offset] - this.offset;
                dst.writeInt(dstStart + i, page.get(position));
            }
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
}
