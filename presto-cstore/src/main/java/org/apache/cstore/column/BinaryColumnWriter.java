package org.apache.cstore.column;

import org.apache.cstore.coder.BufferCoder;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class BinaryColumnWriter<T>
        implements VectorWriter<T>
{
    private StreamWriter writer;
    private final BufferCoder<T> coder;
    private File file;

    public BinaryColumnWriter(VectorWriterFactory writerFactory, BufferCoder<T> coder)
    {
        this.coder = coder;
        this.file = writerFactory.newFile(writerFactory.getName() + ".bin");
        this.writer = new OutputStreamWriter(IOUtil.openFileDataStream(file));
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
    {
        close();
        ByteBuffer buffer = IOUtil.mapFile(file, MapMode.READ_ONLY);
        output.putByteBuffer(buffer);
        return buffer.limit();
    }

    public void close()
    {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
}
