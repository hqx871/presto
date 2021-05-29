package org.apache.cstore.column;

import org.apache.cstore.coder.BufferCoder;

import java.nio.ByteBuffer;

public class BinaryOffsetReader
{
    private BinaryOffsetReader()
    {
    }

    public static <T> BinaryOffsetVector<T> decode(BufferCoder<T> coder, ByteBuffer data)
    {
        int offsetLen = data.getInt(data.limit() - 4);
        data.position(data.limit() - 4 - offsetLen);
        ByteBuffer offsetBuf = data.slice();
        offsetBuf.limit(offsetLen);
        int dataLen = data.getInt(data.limit() - offsetLen - 8);
        data.position(0);
        ByteBuffer dataBuf = data.slice();
        dataBuf.limit(dataLen);

        return new BinaryOffsetVector<>(dataBuf, offsetBuf.asIntBuffer(), coder);
    }
}
