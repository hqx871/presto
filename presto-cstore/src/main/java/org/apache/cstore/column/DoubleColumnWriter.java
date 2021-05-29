package org.apache.cstore.column;

import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.ColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class DoubleColumnWriter
        implements ColumnWriter<Double>
{
    private StreamWriter output;
    private VectorWriterFactory writerFactor;
    private File file;

    public DoubleColumnWriter(VectorWriterFactory writerFactor)
    {
        this.writerFactor = writerFactor;
        this.file = writerFactor.newFile(writerFactor.getName() + ".bin");
        this.output = new OutputStreamWriter(IOUtil.openFileDataStream(file));
    }

    @Override
    public int write(Double value)
    {
        output.putDouble(value);
        return 8;
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
