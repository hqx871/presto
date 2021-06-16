package github.cstore.column;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class ShortColumnReaderFactory
        implements AbstractColumnPlainReader.Factory
{
    @Override
    public PlainReader createPlainReader(int offset, int end, ByteBuffer rawBuffer)
    {
        return new PlainReader(offset, end, rawBuffer);
    }

    @Override
    public NullableReader createNullableReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer)
    {
        return new NullableReader(offset, end, rawBuffer, nullBuffer);
    }

    public static final class PlainReader
            extends AbstractColumnPlainReader
    {
        private final ShortBuffer page;

        public PlainReader(int offset, int end, ByteBuffer rawBuffer)
        {
            super(offset, end, rawBuffer);
            this.page = rawBuffer.asShortBuffer();
        }

        @Override
        public VectorCursor createVectorCursor(int size)
        {
            return new ShortCursor(new int[size]);
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

    public static final class NullableReader
            extends AbstractColumnNullableReader
    {
        private final ShortBuffer page;

        public NullableReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer)
        {
            super(offset, end, rawBuffer, nullBuffer);
            this.page = rawBuffer.asShortBuffer();
        }

        @Override
        public VectorCursor createVectorCursor(int size)
        {
            return new ShortCursor(new int[size]);
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
}
