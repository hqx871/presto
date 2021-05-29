package org.apache.cstore.coder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface BufferCoder<T>
{
    static ByteBuffer concat(ByteBuffer... buffers)
    {
        int size = 0;
        for (int i = 0; i < buffers.length; i++) {
            size += buffers[i].limit();
        }
        ByteBuffer total = ByteBuffer.allocate(size);
        for (int i = 0; i < buffers.length; i++) {
            buffers[i].position(0);
            total.put(buffers[i]);
        }
        return total;
    }

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

    BufferCoder<String> STRING = new BufferCoder<String>()
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

    @Deprecated
    BufferCoder<String> CHAR = new BufferCoder<String>()
    {
        @Override
        public String decode(ByteBuffer data)
        {
            data.position(0);
            char[] chars = new char[data.limit() / 2];
            for (int i = 0; i < chars.length; i++) {
                chars[i] = data.getChar();
            }
            return new String(chars);
        }

        @Override
        public ByteBuffer encode(String str)
        {
            ByteBuffer buffer = ByteBuffer.allocate(str.length() * 2);
            int len = str.length();
            for (int i = 0; i < len; i++) {
                buffer.putChar(str.charAt(i));
            }
            return buffer;
        }
    };

    T decode(ByteBuffer data);

    ByteBuffer encode(T object);
}
