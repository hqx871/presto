package org.apache.cstore.coder;

import java.nio.ByteBuffer;

public interface ValueDecoder<T>
{
    T decode(ByteBuffer data);
}
