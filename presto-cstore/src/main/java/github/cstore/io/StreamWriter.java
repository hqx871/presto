package github.cstore.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public interface StreamWriter
{
    void putByte(byte val);

    void putShort(short val);

    void putInt(int val);

    void putLong(long val);

    void putFloat(float val);

    void putDouble(double val);

    void putChar(char val);

    void putByteBuffer(ByteBuffer val);

    void putShortBuffer(ShortBuffer val);

    void putCharBuffer(CharBuffer val);

    void putIntBuffer(IntBuffer val);

    void putLongBuffer(LongBuffer val);

    void putDoubleBuffer(DoubleBuffer val);

    default void putCharString(String val)
    {
        for (int i = 0; i < val.length(); i++) {
            putChar(val.charAt(i));
        }
    }

    default void putCharArray(char[] val, int from, int to)
    {
        putCharBuffer(CharBuffer.wrap(val, from, to - from));
    }

    default void putLongArray(long[] val, int from, int to)
    {
        putLongBuffer(LongBuffer.wrap(val, from, to - from));
    }

    void flush()
            throws IOException;

    //@Override
    void close()
            throws IOException;

    void delete();

    ByteBuffer map();
}
