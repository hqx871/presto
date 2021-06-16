package github.cstore.column;

import java.nio.ByteBuffer;
import java.util.BitSet;

abstract class AbstractColumnNullableReader
        extends AbstractColumnPlainReader
{
    protected final ByteBuffer nullBuffer;
    private final BitSet nullBitmap;

    protected AbstractColumnNullableReader(int offset, int end, ByteBuffer rawBuffer, ByteBuffer nullBuffer)
    {
        super(offset, end, rawBuffer);
        this.nullBuffer = nullBuffer;
        this.nullBitmap = BitSet.valueOf(nullBuffer);
    }

    public boolean isNull(int position)
    {
        return nullBitmap.get(position);
    }
}
