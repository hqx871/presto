package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public abstract class AbstractColumnWriter<T>
        implements CStoreColumnWriter<T>
{
    protected File file;
    protected StreamWriter streamWriter;
    protected final VectorWriterFactory writerFactory;
    protected final boolean delete;
    protected boolean flushed;

    protected AbstractColumnWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        this.writerFactory = writerFactory;
        this.delete = delete;

        this.file = writerFactory.newFile();
        this.streamWriter = new OutputStreamWriter(writerFactory.createFileStream(file));

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
                bytes += write(readBlock(src, i));
            }
        }
        return bytes;
    }

    @Override
    public T readBlock(Block src, int position)
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
    public final MappedByteBuffer mapFile()
    {
        return IOUtil.mapFile(file, FileChannel.MapMode.READ_ONLY);
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
        if (streamWriter != null) {
            streamWriter.close();
        }
        streamWriter = null;
        if (delete && file != null) {
            file.delete();
        }
        file = null;
    }
}
