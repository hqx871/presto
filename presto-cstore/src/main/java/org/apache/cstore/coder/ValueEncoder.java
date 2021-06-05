package org.apache.cstore.coder;

import java.nio.ByteBuffer;

public interface ValueEncoder<T>
{
    ByteBuffer encode(T object);
}
