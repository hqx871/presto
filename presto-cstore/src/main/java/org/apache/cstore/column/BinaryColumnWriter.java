package org.apache.cstore.column;

import org.apache.cstore.coder.BufferCoder;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class BinaryColumnWriter<T>
        implements CStoreColumnWriter<T>
{
    private StreamWriter writer;
    private final BufferCoder<T> coder;
    private File file;
    private final boolean delete;

    public BinaryColumnWriter(VectorWriterFactory writerFactory, BufferCoder<T> coder, boolean delete)
    {
        this.coder = coder;
        this.file = writerFactory.newFile(writerFactory.getName() + ".bin");
        this.writer = new OutputStreamWriter(IOUtil.openFileDataStream(file));
        this.delete = delete;
    }

    @Override
    public int write(T value)
    {
        ByteBuffer byteBuffer = coder.encode(value);
        writer.putByteBuffer(byteBuffer);
        return byteBuffer.limit();
    }

    @Override
    public int flushTo(StreamWriter output)
            throws IOException
    {
        flush();
        ByteBuffer buffer = IOUtil.mapFile(file, MapMode.READ_ONLY);
        output.putByteBuffer(buffer);
        return buffer.limit();
    }

    @Override
    public void flush()
            throws IOException
    {
        writer.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (delete && file != null) {
            file.delete();
        }
        file = null;
        if (writer != null) {
            writer.close();
        }
        writer = null;
    }
}
