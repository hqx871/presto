package github.cstore.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public abstract class DataOutputStreamWriter
        implements StreamWriter
{
    private DataOutputStream output;

    public DataOutputStreamWriter(DataOutputStream output)
    {
        this.output = output;
    }

    @Override
    public void putByte(byte val)
    {
        try {
            output.writeByte(val);
        }
        catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void putShort(short val)
    {
        try {
            output.writeShort(val);
        }
        catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void putInt(int val)
    {
        try {
            output.writeInt(val);
        }
        catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void putLong(long val)
    {
        try {
            output.writeLong(val);
        }
        catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void putFloat(float val)
    {
        try {
            output.writeFloat(val);
        }
        catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void putDouble(double val)
    {
        try {
            output.writeDouble(val);
        }
        catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void putChar(char val)
    {
        try {
            output.writeChar(val);
        }
        catch (IOException e) {
            handleIOException(e);
        }
    }

    @Override
    public void putByteBuffer(ByteBuffer val)
    {
        while (val.hasRemaining()) {
            try {
                output.writeByte(val.get());
            }
            catch (IOException e) {
                handleIOException(e);
            }
        }
    }

    @Override
    public void putShortBuffer(ShortBuffer val)
    {
        while (val.hasRemaining()) {
            try {
                output.writeShort(val.get());
            }
            catch (IOException e) {
                handleIOException(e);
            }
        }
    }

    @Override
    public void putCharBuffer(CharBuffer val)
    {
        while (val.hasRemaining()) {
            try {
                output.writeChar(val.get());
            }
            catch (IOException e) {
                handleIOException(e);
            }
        }
    }

    @Override
    public void putIntBuffer(IntBuffer val)
    {
        while (val.hasRemaining()) {
            try {
                output.writeInt(val.get());
            }
            catch (IOException e) {
                handleIOException(e);
            }
        }
    }

    @Override
    public void putLongBuffer(LongBuffer val)
    {
        while (val.hasRemaining()) {
            try {
                output.writeLong(val.get());
            }
            catch (IOException e) {
                handleIOException(e);
            }
        }
    }

    @Override
    public void putDoubleBuffer(DoubleBuffer val)
    {
        while (val.hasRemaining()) {
            try {
                output.writeDouble(val.get());
            }
            catch (IOException e) {
                handleIOException(e);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (output != null) {
            output.close();
        }
        output = null;
    }

    @Override
    public void flush()
            throws IOException
    {
        if (output != null) {
            output.flush();
        }
    }

    private void handleIOException(IOException e)
    {
        throw new RuntimeException(e);
    }
}