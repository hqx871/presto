package org.apache.cstore.column;

import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.OutputStreamWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;
import org.apache.cstore.util.IOUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class ByteRLEColumnWriter
        implements CStoreColumnWriter<Byte>
{
    private final StreamWriter output;
    private byte currentValue;
    private byte currentLength;
    private final File file;

    public ByteRLEColumnWriter(VectorWriterFactory writerFactor, int count)
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
    {
        close();
        ByteBuffer buffer = IOUtil.mapFile(file, MapMode.READ_ONLY);
        output.putByteBuffer(buffer);
        return buffer.limit();
    }

    private int flush()
    {
        if (currentLength > 0) {
            output.putByte(currentLength);
            output.putByte(currentValue);
            return 2;
        }
        return 0;
    }

    @Override
    public void close()
    {
        flush();
        if (output != null) {
            output.close();
        }
    }
}
