package org.apache.cstore.column;

import org.apache.cstore.coder.BufferCoder;
import org.apache.cstore.io.StreamWriter;
import org.apache.cstore.io.ColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;

public class BinaryOffsetWriter<T>
        implements ColumnWriter<T>
{
    private final ColumnWriter<Integer> offsetWriter;
    private final ColumnWriter<T> dataWriter;
    private int offset;

    public BinaryOffsetWriter(VectorWriterFactory writerFactor, BufferCoder<T> coder)
    {
        this.offsetWriter = new IntColumnWriter(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + "-offset"));
        this.dataWriter = new BinaryColumnWriter<>(new VectorWriterFactory(writerFactor.getDir(), writerFactor.getName() + "-data"), coder);
        this.offset = 0;

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
    {
        close();

        int dataSize = dataWriter.flushTo(output);
        output.putInt(dataSize);
        int offsetSize = offsetWriter.flushTo(output);
        output.putInt(offsetSize);

        return offsetSize + dataSize + 8;
    }

    @Override
    public void close()
    {
        offsetWriter.close();
        dataWriter.close();
    }
}
