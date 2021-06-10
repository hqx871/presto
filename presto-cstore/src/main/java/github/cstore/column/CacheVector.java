package github.cstore.column;

import github.cstore.coder.BufferCoder;

public abstract class CacheVector<T>
{
    private T[] data;

    private BinaryOffsetVector<T> buffer;

    private BufferCoder<T> coder;

    public CacheVector(BinaryOffsetVector<T> buffer, T[] data, BufferCoder<T> coder)
    {
        this.buffer = buffer;
        this.data = data;
        this.coder = coder;
    }

    //@Override
    public final T readObject(int index)
    {
        if (data[index] == null) {
            data[index] = buffer.readObject(index);
        }
        return data[index];
    }

    public BinaryOffsetVector<T> getBuffer()
    {
        return buffer;
    }

    //@Override
    public int count()
    {
        return buffer.count();
    }

    //@Override
//    public int vectorId() {
//        return buffer.vectorId();
//    }
}
