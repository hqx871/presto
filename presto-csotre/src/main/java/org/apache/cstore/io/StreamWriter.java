package org.apache.cstore.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public abstract class StreamWriter
{
    public abstract void putByte(byte val);

    public abstract void putShort(short val);

    public abstract void putInt(int val);

    public abstract void putLong(long val);

    public abstract void putFloat(float val);

    public abstract void putDouble(double val);

    public abstract void putChar(char val);

    public abstract void putByteBuffer(ByteBuffer val);

    public abstract void putShortBuffer(ShortBuffer val);

    public abstract void putCharBuffer(CharBuffer val);

    public abstract void putIntBuffer(IntBuffer val);

    public abstract void putLongBuffer(LongBuffer val);

    public abstract void putDoubleBuffer(DoubleBuffer val);

    public void putCharString(String val)
    {
        for (int i = 0; i < val.length(); i++) {
            putChar(val.charAt(i));
        }
    }

    public void putCharArray(char[] val, int from, int to)
    {
        putCharBuffer(CharBuffer.wrap(val, from, to - from));
    }

    public void putLongArray(long[] val, int from, int to)
    {
        putLongBuffer(LongBuffer.wrap(val, from, to - from));
    }

    //@Override
    public void close()
    {
    }
}
