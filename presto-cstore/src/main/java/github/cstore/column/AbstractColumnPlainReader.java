package github.cstore.column;

import java.nio.ByteBuffer;

abstract class AbstractColumnPlainReader
        implements CStoreColumnReader
{
    protected final int offset;
    protected final int end;
    protected final ByteBuffer rawBuffer;
    protected final int rowCount;

    protected AbstractColumnPlainReader(int offset, int end, ByteBuffer rawBuffer)
    {
        this.offset = offset;
        this.end = end;
        this.rawBuffer = rawBuffer;
        this.rowCount = end - offset;
    }

    public ByteBuffer getRawBuffer()
    {
        return rawBuffer;
    }

    @Override
    public void setup()
    {
    }

    @Override
    public final int getRowCount()
    {
        return rowCount;
    }

    @Override
    public void close()
    {
    }

    public interface Factory
    {
        AbstractColumnPlainReader createPlainReader(int offset, int end, ByteBuffer rawBuffer);

        AbstractColumnNullableReader createNullableReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer);
    }
}
