package org.apache.cstore.column;

import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class IntColumnWriter
        implements CStoreColumnWriter<Integer>
{
    private StreamWriter output;
    private File file;
    private boolean delete;

    public IntColumnWriter(VectorWriterFactory writerFactory, boolean delete)
    {
        this.file = writerFactory.newFile(writerFactory.getName() + ".bin");
        this.output = new OutputStreamWriter(IOUtil.openFileDataStream(file));
        this.delete = delete;
    }

    @Override
    public int write(Integer value)
    {
        output.putInt(value);
        return Integer.BYTES;
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
        output.flush();
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
        if (output != null) {
            output.close();
        }
        output = null;
    }
}
