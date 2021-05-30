package org.apache.cstore.column;

import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class BinaryOffsetVector<T>
{
    private final ByteBuffer valueBuffer;
    private final IntBuffer offsetBuffer;
    private final BufferCoder<T> coder;

    public BinaryOffsetVector(ByteBuffer valueBuffer, IntBuffer offsetBuffer, BufferCoder<T> coder)
    {
        this.valueBuffer = valueBuffer;
        this.offsetBuffer = offsetBuffer;
        this.coder = coder;
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
}
