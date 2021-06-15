package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.coder.ValueEncoder;
import github.cstore.io.StreamWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

@Deprecated
public class BinaryFixColumnWriter<T>
        extends AbstractColumnWriter<T>
{
    private final ValueEncoder<T> coder;

    public BinaryFixColumnWriter(String name, StreamWriter streamWriter, ValueEncoder<T> coder, boolean delete)
    {
        super(name, streamWriter, delete);
        this.coder = coder;
    }

    @Override
    public int doWrite(T value)
    {
        ByteBuffer byteBuffer = coder.encode(value);
        valueStreamWriter.putByteBuffer(byteBuffer);
        return byteBuffer.limit();
    }

    @Override
    protected void doFlush()
            throws IOException
    {
        valueStreamWriter.flush();
    }

    @Override
    public int writeNull()
    {
        return 0;
    }

    @Override
    public T readValue(Block src, int position)
    {
        throw new UnsupportedOperationException();
    }
}
