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

public class ByteColumnPlainWriter
        implements CStoreColumnWriter<Byte>
{
    private StreamWriter output;
    private File file;
    private boolean delete;

    public ByteColumnPlainWriter(VectorWriterFactory writerFactor, boolean delete)
    {
        this.file = writerFactor.newFile(writerFactor.getName() + ".bin");
        this.output = new OutputStreamWriter(IOUtil.openFileDataStream(file));
        this.delete = delete;
    }

    @Override
    public int write(Byte value)
    {
        output.putByte(value);
        return Byte.BYTES;
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
        if (output != null) {
            output.close();
        }
        file = null;
        output = null;
    }
}
