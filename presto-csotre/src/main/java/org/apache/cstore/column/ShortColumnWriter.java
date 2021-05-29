package org.apache.cstore.column;

import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class ShortColumnWriter
        implements VectorWriter<Short>
{
    private StreamWriter output;
    private File file;

    public ShortColumnWriter(VectorWriterFactory writerFactory)
    {
        this.file = writerFactory.newFile(writerFactory.getName() + ".bin");
        this.output = new OutputStreamWriter(IOUtil.openFileDataStream(file));
    }

    @Override
    public int write(Short value)
    {
        output.putShort(value);
        return 2;
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
            file = null;
        }
    }
}
