package org.apache.cstore.column;

import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.ColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class IntColumnWriter
        implements ColumnWriter<Integer>
{
    private StreamWriter output;
    private File file;

    public IntColumnWriter(VectorWriterFactory writerFactory)
    {
        this.file = writerFactory.newFile(writerFactory.getName() + ".bin");
        this.output = new OutputStreamWriter(IOUtil.openFileDataStream(file));
    }

    @Override
    public int write(Integer value)
    {
        output.putInt(value);
        return 4;
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
