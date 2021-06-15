package github.cstore.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public interface StreamWriter
        extends Closeable
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

    void flush()
            throws IOException;

    void delete();

    ByteBuffer toByteBuffer();

    void reset();
}
