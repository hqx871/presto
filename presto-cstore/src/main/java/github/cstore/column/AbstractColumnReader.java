package github.cstore.column;

import java.nio.ByteBuffer;

public abstract class AbstractColumnReader
        implements CStoreColumnReader
{
    protected final BinaryOffsetVector<ByteBuffer> chunks;

    protected AbstractColumnReader(BinaryOffsetVector<ByteBuffer> chunks)
    {
        this.chunks = chunks;
    }
}
