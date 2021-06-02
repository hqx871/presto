package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

public interface CStoreColumnReader
{
    void setup();

    VectorCursor createVectorCursor(int size);

    int read(int[] positions, int offset, int size, VectorCursor dst);

    int read(int offset, int size, VectorCursor dst);

    @Deprecated
    int read(int[] positions, int offset, int size, BlockBuilder dst);

    @Deprecated
    int read(int offset, int size, BlockBuilder dst);

    void close();
}
