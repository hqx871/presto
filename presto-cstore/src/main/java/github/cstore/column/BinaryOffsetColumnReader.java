package github.cstore.column;

import github.cstore.coder.BufferCoder;
import github.cstore.coder.ValueDecoder;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.function.Function;

public class BinaryOffsetColumnReader<T>
        implements CStoreColumnReader, BinaryVector<T>
{
    private final ByteBuffer valueBuffer;
    private final IntBuffer offsetBuffer;
    private final ValueDecoder<T> coder;

    public BinaryOffsetColumnReader(ByteBuffer valueBuffer, IntBuffer offsetBuffer, ValueDecoder<T> coder)
    {
        this.valueBuffer = valueBuffer;
        this.offsetBuffer = offsetBuffer;
        this.coder = coder;
    }

    public static <T> BinaryOffsetColumnReader<T> decode(BufferCoder<T> coder, ByteBuffer buffer)
    {
        return decode(count -> coder, buffer);
    }

    @Deprecated
    public static <T> BinaryOffsetColumnReader<T> decode(Function<Integer, BufferCoder<T>> coder, ByteBuffer buffer)
    {
        int offsetLen = buffer.getInt(buffer.limit() - Integer.BYTES);
        int valueLen = buffer.getInt(buffer.limit() - 2 * Integer.BYTES);
        buffer.position(buffer.limit() - 2 * Integer.BYTES - offsetLen);
        ByteBuffer offsetBuffer = buffer.slice();
        offsetBuffer.limit(offsetLen);
        buffer.position(buffer.limit() - 2 * Integer.BYTES - offsetLen - valueLen);
        ByteBuffer valueBuffer = buffer.slice();
        valueBuffer.limit(valueLen);

        IntBuffer offsetIntBuffer = offsetBuffer.asIntBuffer();
        return new BinaryOffsetColumnReader<>(valueBuffer, offsetIntBuffer, coder.apply(offsetIntBuffer.limit() - 1));
    }

    public T readObject(int position)
    {
        ByteBuffer value = readByteBuffer(position);
        return coder.decode(value);
    }

    @Override
    public ByteBuffer readByteBuffer(int position)
    {
        int offset = offsetBuffer.get(position);
        int size = offsetBuffer.get(position + 1) - offset;
        valueBuffer.position(offset);
        ByteBuffer value = valueBuffer.slice();
        value.limit(size);
        return value;
    }

    public ByteBuffer getValueBuffer()
    {
        return valueBuffer;
    }

    public IntBuffer getOffsetBuffer()
    {
        return offsetBuffer;
    }

    @Deprecated
    public int count()
    {
        return offsetBuffer.limit() - 1;
    }

    public BinaryOffsetColumnReader<T> duplicate()
    {
        return new BinaryOffsetColumnReader<>(valueBuffer.duplicate(), offsetBuffer.duplicate(), coder);
    }

    @Override
    public void setup()
    {
    }

    @Override
    public int getRowCount()
    {
        return count();
    }

    @Override
    public VectorCursor createVectorCursor(int size)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
    }

    public static class Builder<T>
            implements CStoreColumnReader.Builder
    {
        private final ByteBuffer valueBuffer;
        private final IntBuffer offsetBuffer;
        private final ValueDecoder<T> coder;

        public Builder(ByteBuffer valueBuffer, IntBuffer offsetBuffer, ValueDecoder<T> coder)
        {
            this.valueBuffer = valueBuffer;
            this.offsetBuffer = offsetBuffer;
            this.coder = coder;
        }

        @Override
        public BinaryOffsetColumnReader<T> build()
        {
            return new BinaryOffsetColumnReader<>(valueBuffer, offsetBuffer, coder);
        }
    }
}
