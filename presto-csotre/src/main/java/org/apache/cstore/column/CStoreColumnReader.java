package org.apache.cstore.column;

import com.facebook.presto.common.block.BlockBuilder;

public interface CStoreColumnReader
{
    void setup();

    int read(int[] positions, int offset, int size, BlockBuilder dst);

    int read(int offset, int size, BlockBuilder dst);

    void close();
}
