package github.cstore.column;

public interface CStoreColumnReader
{
    void setup();

    int getRowCount();

    VectorCursor createVectorCursor(int size);

    default int read(int[] positions, int offset, int size, VectorCursor dst, int dstOffset)
    {
        throw new UnsupportedOperationException();
    }

    default int read(int offset, int size, VectorCursor dst, int dstOffset)
    {
        throw new UnsupportedOperationException();
    }

    void close();

    interface Builder
    {
        CStoreColumnReader build();
    }
}
