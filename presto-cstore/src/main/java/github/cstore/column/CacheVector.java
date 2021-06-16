package github.cstore.column;

import java.nio.ByteBuffer;

public class CacheVector<T>
        implements BinaryVector<T>
{
    private final T[] data;

    private final BinaryVector<T> buffer;

    public CacheVector(BinaryVector<T> buffer, T[] data)
    {
        this.buffer = buffer;
        this.data = data;
    }

    @Override
    public final T readObject(int index)
    {
        if (data[index] == null) {
            data[index] = buffer.readObject(index);
        }
        return data[index];
    }

    @Override
    public ByteBuffer readByteBuffer(int position)
    {
        return buffer.readByteBuffer(position);
    }
}
