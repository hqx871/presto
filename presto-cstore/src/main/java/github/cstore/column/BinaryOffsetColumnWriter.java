package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.coder.ValueEncoder;
import github.cstore.io.StreamWriter;
import github.cstore.io.StreamWriterFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryOffsetColumnWriter<T>
        extends AbstractColumnWriter<T>
{
    private StreamWriter offsetWriter;
    private StreamWriter dataWriter;
    private int offset;
    private final ValueEncoder<T> coder;
    private final StreamWriterFactory writerFactory;

    public BinaryOffsetColumnWriter(String name, StreamWriter streamWriter, StreamWriterFactory writerFactory, ValueEncoder<T> coder, boolean delete)
    {
        super(name, streamWriter, delete);
        this.coder = coder;
        this.writerFactory = writerFactory;
        this.offsetWriter = writerFactory.createWriter(name + ".offset", true);
        this.dataWriter = writerFactory.createWriter(name + ".data", true);
        this.offset = 0;

        offsetWriter.putInt(offset);
    }

    @Override
    public int doWrite(T value)
    {
        ByteBuffer buffer = coder.encode(value);
        int size = buffer.remaining();
        dataWriter.putByteBuffer(buffer);
        offset += size;
        offsetWriter.putInt(offset);
        return Integer.BYTES + size;
    }

    @Override
    protected void doFlush()
            throws IOException
    {
        ByteBuffer dataBuffer = dataWriter.toByteBuffer();
        int dataSize = dataBuffer.remaining();
        valueStreamWriter.putByteBuffer(dataBuffer);
        ByteBuffer offsetBuffer = offsetWriter.toByteBuffer();
        int offsetSize = offsetBuffer.remaining();
        valueStreamWriter.putByteBuffer(offsetBuffer);
        valueStreamWriter.putInt(dataSize);
        valueStreamWriter.putInt(offsetSize);
        valueStreamWriter.flush();
    }

    @Override
    public int writeNull()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public T readValue(Block src, int position)
    {
        throw new UnsupportedOperationException();
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

    @Override
    public void reset()
    {
        this.offsetWriter.reset();
        this.dataWriter.reset();
        this.offset = 0;
        super.reset();

        offsetWriter.putInt(offset);
    }
}
