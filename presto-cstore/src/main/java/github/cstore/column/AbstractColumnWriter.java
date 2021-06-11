package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class AbstractColumnWriter<T>
        implements CStoreColumnWriter<T>
{
    protected StreamWriter streamWriter;
    protected final boolean delete;
    protected boolean flushed;
    protected final String name;

    protected AbstractColumnWriter(String name, StreamWriter streamWriter, boolean delete)
    {
        this.name = name;
        this.delete = delete;
        this.streamWriter = streamWriter;
        flushed = false;
    }

    @Override
    public int write(Block src, int size)
    {
        int bytes = 0;
        for (int i = 0; i < size; i++) {
            if (src.isNull(i)) {
                bytes += writeNull();
            }
            else {
                bytes += write(readBlockValue(src, i));
            }
        }
        return bytes;
    }

    @Override
    public T readBlockValue(Block src, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int writeNull()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int appendTo(StreamWriter output)
            throws IOException
    {
        ByteBuffer buffer = mapFile();
        output.putByteBuffer(buffer);
        return buffer.limit();
    }

    @Override
    public final ByteBuffer mapFile()
    {
        return streamWriter.map();
    }

    @Override
    public final void flush()
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
        streamWriter.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (delete && streamWriter != null) {
            streamWriter.delete();
        }
        streamWriter = null;
    }
}
