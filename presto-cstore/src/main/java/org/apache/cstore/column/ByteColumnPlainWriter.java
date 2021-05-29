package org.apache.cstore.column;

import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class ByteColumnPlainWriter
        implements VectorWriter<Byte>
{
    private StreamWriter output;
    private final File file;

    public ByteColumnPlainWriter(VectorWriterFactory writerFactor)
    {
        this.file = writerFactor.newFile(writerFactor.getName() + ".bin");
        this.output = new OutputStreamWriter(IOUtil.openFileDataStream(file));
    }

    @Override
    public int write(Byte value)
    {
        output.putByte(value);
        return 1;
    }

    @Override
    public int flushTo(StreamWriter output)
    {
        close();
        ByteBuffer buffer = IOUtil.mapFile(file, MapMode.READ_ONLY);
        output.putByteBuffer(buffer);
        return buffer.limit();
    }

    @Override
    public void close()
    {
        if (output != null) {
            output.close();
            output = null;
        }
    }
}
