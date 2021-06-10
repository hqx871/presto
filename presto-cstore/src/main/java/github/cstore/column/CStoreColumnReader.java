package github.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

public interface CStoreColumnReader
{
    void setup();

    int getRowCount();

    VectorCursor createVectorCursor(int size);

    int read(int[] positions, int offset, int size, VectorCursor dst);

    int read(int offset, int size, VectorCursor dst);

    @Deprecated
    default int read(int[] positions, int offset, int size, BlockBuilder dst)
    {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default int read(int offset, int size, BlockBuilder dst)
    {
        throw new UnsupportedOperationException();
    }

    void close();

    interface Builder
    {
        CStoreColumnReader build();
    }
}
