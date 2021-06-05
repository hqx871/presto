package org.apache.cstore.column;

import org.apache.cstore.coder.ValueEncoder;
import org.apache.cstore.io.CStoreColumnWriter;
import org.apache.cstore.io.VectorWriterFactory;

import java.io.IOException;

public class BinaryOffsetColumnWriter<T>
        extends AbstractColumnWriter<T>
{
    private CStoreColumnWriter<Integer> offsetWriter;
    private CStoreColumnWriter<T> dataWriter;
    private int offset;

    public BinaryOffsetColumnWriter(VectorWriterFactory writerFactory, ValueEncoder<T> coder, boolean delete)
    {
        super(writerFactory, delete);
        this.offsetWriter = new IntColumnPlainWriter(new VectorWriterFactory(writerFactory.getDir(), writerFactory.getName() + ".offset", "bin"), true);
        this.dataWriter = new BinaryFixColumnWriter<>(new VectorWriterFactory(writerFactory.getDir(), writerFactory.getName() + ".data", "bin"), coder, true);
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
    protected void doFlush()
            throws IOException
    {
        offsetWriter.flush();
        dataWriter.flush();

        int dataSize = dataWriter.appendTo(streamWriter);
        int offsetSize = offsetWriter.appendTo(streamWriter);
        streamWriter.putInt(dataSize);
        streamWriter.putInt(offsetSize);
        streamWriter.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        if (offsetWriter != null) {
            offsetWriter.close();
        }
        offsetWriter = null;
        if (dataWriter != null) {
            dataWriter.close();
        }
        dataWriter = null;
        super.close();
    }
}
