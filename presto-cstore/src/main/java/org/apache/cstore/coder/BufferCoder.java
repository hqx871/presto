package org.apache.cstore.coder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface BufferCoder<T>
        extends ValueEncoder<T>, ValueDecoder<T>
{
    BufferCoder<String> UTF8 = new BufferCoder<String>()
    {
        @Override
        public String decode(ByteBuffer data)
        {
            data.position(0);
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public ByteBuffer encode(String object)
        {
            return ByteBuffer.wrap(object.getBytes(StandardCharsets.UTF_8));
        }
    };

    BufferCoder<ByteBuffer> BYTE_BUFFER = new BufferCoder<ByteBuffer>()
    {
        @Override
        public ByteBuffer decode(ByteBuffer data)
        {
            return data;
        }

        @Override
        public ByteBuffer encode(ByteBuffer object)
        {
            return object;
        }
    };

    ByteBuffer encode(T object);

    T decode(ByteBuffer data);
}
