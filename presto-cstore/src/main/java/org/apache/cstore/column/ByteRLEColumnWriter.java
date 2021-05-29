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

public class ByteRLEColumnWriter
        implements CStoreColumnWriter<Byte>
{
    private StreamWriter output;
    private byte currentValue;
    private byte currentLength;
    private File file;
    private boolean delete;

    public ByteRLEColumnWriter(VectorWriterFactory writerFactor, int count, boolean delete)
    {
        this.file = writerFactor.newFile(writerFactor.getName() + ".bin");
        this.output = new OutputStreamWriter(IOUtil.openFileDataStream(file));
        output.putInt(count);
    }

    @Override
    public int write(Byte value)
    {
        if (currentLength == 0) {
            currentLength = 1;
            currentValue = value;
            return 0;
        }
        else if (currentValue == value) {
            currentLength++;
            if (currentLength == Byte.MAX_VALUE) {
                output.putByte(currentLength);
                output.putByte(currentValue);

                currentLength = 0;
            }
            return 0;
        }
        else {
            output.putByte(currentLength);
            output.putByte(currentValue);

            int size = 2 * currentLength;
            currentValue = value;
            currentLength = 1;
            return size;
        }
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
        if (currentLength > 0) {
            output.putByte(currentLength);
            output.putByte(currentValue);
        }
        output.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (output != null) {
            output.close();
        }
        if (delete && file != null) {
            file.delete();
        }
        output = null;
        file = null;
    }
}
