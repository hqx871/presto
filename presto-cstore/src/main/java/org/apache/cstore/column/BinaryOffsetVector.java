package org.apache.cstore.column;

import org.apache.cstore.coder.BufferCoder;
import org.apache.cstore.coder.ValueDecoder;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class BinaryOffsetVector<T>
{
    private final ByteBuffer valueBuffer;
    private final IntBuffer offsetBuffer;
    private final ValueDecoder<T> coder;

    public BinaryOffsetVector(ByteBuffer valueBuffer, IntBuffer offsetBuffer, ValueDecoder<T> coder)
    {
        this.valueBuffer = valueBuffer;
        this.offsetBuffer = offsetBuffer;
        this.coder = coder;
    }

    public static <T> BinaryOffsetVector<T> decode(BufferCoder<T> coder, ByteBuffer buffer)
    {
        int offsetLen = buffer.getInt(buffer.limit() - Integer.BYTES);
        int valueLen = buffer.getInt(buffer.limit() - 2 * Integer.BYTES);
        buffer.position(buffer.limit() - 2 * Integer.BYTES - offsetLen);
        ByteBuffer offsetBuffer = buffer.slice();
        offsetBuffer.limit(offsetLen);
        buffer.position(buffer.limit() - 2 * Integer.BYTES - offsetLen - valueLen);
        ByteBuffer valueBuffer = buffer.slice();
        valueBuffer.limit(valueLen);

        return new BinaryOffsetVector<>(valueBuffer, offsetBuffer.asIntBuffer(), coder);
    }

    public T readObject(int position)
    {
        ByteBuffer value = readBuffer(position);
        return coder.decode(value);
    }

    public ByteBuffer readBuffer(int position)
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

    public int count()
    {
        return offsetBuffer.limit() - 1;
    }

    public BinaryOffsetVector<T> duplicate()
    {
        return new BinaryOffsetVector<>(valueBuffer.duplicate(), offsetBuffer.duplicate(), coder);
    }
}
