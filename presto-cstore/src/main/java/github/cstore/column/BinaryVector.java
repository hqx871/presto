package github.cstore.column;

import java.nio.ByteBuffer;

public interface BinaryVector<T>
{
    T readObject(int position);

    ByteBuffer readByteBuffer(int position);
}
