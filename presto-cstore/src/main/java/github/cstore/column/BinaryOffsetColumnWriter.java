package github.cstore.column;

import github.cstore.coder.ValueEncoder;
import github.cstore.io.StreamWriterFactory;

import java.io.IOException;

public class BinaryOffsetColumnWriter<T>
        extends AbstractColumnWriter<T>
{
    private CStoreColumnWriter<Integer> offsetWriter;
    private CStoreColumnWriter<T> dataWriter;
    private int offset;

    public BinaryOffsetColumnWriter(String name, StreamWriterFactory writerFactory, ValueEncoder<T> coder, boolean delete)
    {
        super(name, writerFactory.createWriter(name + ".bin", delete), delete);
        this.offsetWriter = new IntColumnPlainWriter(name + ".offset", writerFactory, true);
        this.dataWriter = new BinaryFixColumnWriter<>(name + ".data", writerFactory, coder, delete);
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
