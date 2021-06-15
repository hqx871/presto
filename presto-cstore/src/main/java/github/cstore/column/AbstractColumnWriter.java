package github.cstore.column;

import github.cstore.io.StreamWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class AbstractColumnWriter<T>
        implements CStoreColumnWriter<T>
{
    protected StreamWriter valueStreamWriter;
    protected final boolean delete;
    protected boolean flushed;
    protected final String name;
    protected int rowCount;

    protected AbstractColumnWriter(String name, StreamWriter valueStreamWriter, boolean delete)
    {
        this.name = name;
        this.delete = delete;
        this.valueStreamWriter = valueStreamWriter;
    }

    @Override
    public final int write(T value)
    {
        if (value == null) {
            rowCount++;
            return writeNull();
        }
        else {
            rowCount++;
            return doWrite(value);
        }
    }

    protected abstract int doWrite(T value);

    @Override
    public final ByteBuffer mapBuffer()
    {
        try {
            flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return valueStreamWriter.toByteBuffer();
    }

    protected final void flush()
            throws IOException
    {
        if (!flushed) {
            doFlush();
        }
        flushed = true;
    }

    protected void doFlush()
            throws IOException
    {
        valueStreamWriter.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (delete && valueStreamWriter != null) {
            valueStreamWriter.delete();
        }
        valueStreamWriter = null;
    }

    @Override
    public final int getRowCount()
    {
        return rowCount;
    }

    @Override
    public void reset()
    {
        rowCount = 0;
        flushed = false;
        valueStreamWriter.reset();
    }
}
