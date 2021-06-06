package org.apache.cstore.util;

import java.nio.ByteBuffer;

public class MemoryManager
{
    public ByteBuffer allocate(int capacity)
    {
        return ByteBuffer.allocate(capacity);
    }

    public ByteBuffer allocateDirect(int capacity)
    {
        return ByteBuffer.allocateDirect(capacity);
    }
}
