package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class NullableColumnWriter<T>
        extends AbstractColumnWriter<T>
{
    private final BitSet nullSet;
    private final CStoreColumnWriter<T> delegate;

    public NullableColumnWriter(String name, StreamWriter valueStreamWriter,
            CStoreColumnWriter<T> delegate, boolean delete)
    {
        super(name, valueStreamWriter, delete);
        this.delegate = delegate;
        this.nullSet = new BitSet();
    }

    @Override
    public int writeNull()
    {
        nullSet.set(rowCount);
        return delegate.writeNull();
    }

    @Override
    public T readValue(Block src, int position)
    {
        return delegate.readValue(src, position);
    }

    @Override
    protected int doWrite(T value)
    {
        return delegate.write(value);
    }

    protected void doFlush()
            throws IOException
    {
        if (nullSet.isEmpty()) {
            valueStreamWriter.putInt(0);
        }
        else {
            ByteBuffer bitmap = ByteBuffer.wrap(nullSet.toByteArray());
            valueStreamWriter.putInt(bitmap.remaining());
            valueStreamWriter.putByteBuffer(bitmap);
        }

        ByteBuffer valueBuffer = delegate.mapBuffer();
        valueStreamWriter.putByteBuffer(valueBuffer);
        super.doFlush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        delegate.close();
        if (delete && valueStreamWriter != null) {
            valueStreamWriter.delete();
        }
        valueStreamWriter = null;
    }

    @Override
    public void reset()
    {
        //flushed = false;
        nullSet.clear();
        delegate.reset();
        super.reset();
    }
}
