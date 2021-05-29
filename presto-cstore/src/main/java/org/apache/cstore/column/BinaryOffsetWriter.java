package org.apache.cstore.column;

import org.apache.cstore.coder.BufferCoder;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.VectorWriterFactory;

import java.io.IOException;

public class BinaryOffsetWriter<T>
        implements CStoreColumnWriter<T>
{
    private CStoreColumnWriter<Integer> offsetWriter;
    private CStoreColumnWriter<T> dataWriter;
    private int offset;
    private boolean delete;

    public BinaryOffsetWriter(VectorWriterFactory writerFactor, BufferCoder<T> coder, boolean delete)
    {
        this.offsetWriter = new IntColumnWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + ".offset"), true);
        this.dataWriter = new BinaryColumnWriter<>(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + ".data"), coder, true);
        this.offset = 0;
        this.delete = delete;

        offsetWriter.write(offset);
    }

    @Override
    public int write(T value)
    {
        int size = dataWriter.write(value);
        offset += size;
        offsetWriter.write(offset);

        return 0;
    }

    @Override
    public int flushTo(StreamWriter output)
            throws IOException
    {
        flush();

        int dataSize = dataWriter.flushTo(output);
        output.putInt(dataSize);
        int offsetSize = offsetWriter.flushTo(output);
        output.putInt(offsetSize);

        return offsetSize + dataSize + Integer.BYTES * 2;
    }

    @Override
    public void flush()
            throws IOException
    {
        offsetWriter.flush();
        dataWriter.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (offsetWriter != null) {
            offsetWriter.close();
        }
        if (dataWriter != null) {
            dataWriter.close();
        }
        offsetWriter = null;
        dataWriter = null;
    }
}
