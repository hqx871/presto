package org.apache.cstore.column;

import com.facebook.presto.common.type.TinyintType;
import io.airlift.compress.Decompressor;
import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;

public final class ByteColumnZipReader
        extends AbstractColumnZipReader
        implements IntVector
{
    private final BytePageReader pageReader;

    public ByteColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            TinyintType type)
    {
        this(rowCount, pageSize, chunks, decompressor,
                new BytePageReader(0, 0, ByteBuffer.wrap(new byte[0]), -1), type);
    }

    private ByteColumnZipReader(int rowCount,
            int pageSize,
            BinaryOffsetVector<ByteBuffer> chunks,
            Decompressor decompressor,
            BytePageReader pageReader,
            TinyintType type)
    {
        super(rowCount, chunks, decompressor, pageSize, type, pageReader);
        this.pageReader = pageReader;
    }

    public static ByteColumnZipReader decode(int rowCount, int pageSize, ByteBuffer buffer, Decompressor decompressor, TinyintType type)
    {
        BinaryOffsetVector<ByteBuffer> chunks = BinaryOffsetVector.decode(BufferCoder.BYTE_BUFFER, buffer);
        return new ByteColumnZipReader(rowCount, pageSize, chunks, decompressor, type);
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

    @Override
    public int readInt(int position)
    {
        loadPage(position);
        return pageReader.readInt(position);
    }

    private static class BytePageReader
            extends PageReader
    {
        private final ByteBuffer page;

        private BytePageReader(int offset, int end, ByteBuffer rawBuffer, int pageNum)
        {
            super(offset, end, rawBuffer, pageNum);
            this.page = rawBuffer;
        }

        @Override
        public void read(int[] positions, int offset, int size, VectorCursor dst, int dstStart)
        {
            for (int i = 0; i < size; i++) {
                int position = positions[i + offset] - this.offset;
                dst.writeByte(dstStart + i, page.get(position));
            }
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
}
