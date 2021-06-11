package github.cstore.column;

import github.cstore.coder.ValueEncoder;
import github.cstore.io.StreamWriterFactory;

import java.nio.ByteBuffer;

public class BinaryFixColumnWriter<T>
        extends AbstractColumnWriter<T>
{
    private final ValueEncoder<T> coder;

    public BinaryFixColumnWriter(String name, StreamWriterFactory writerFactory, ValueEncoder<T> coder, boolean delete)
    {
        super(name, writerFactory, delete);
        this.coder = coder;
    }

    @Override
    public int write(T value)
    {
        ByteBuffer byteBuffer = coder.encode(value);
        streamWriter.putByteBuffer(byteBuffer);
        return byteBuffer.limit();
    }
}
