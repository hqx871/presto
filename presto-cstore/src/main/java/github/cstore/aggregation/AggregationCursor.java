package github.cstore.aggregation;

import github.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

public interface AggregationCursor
{
    VectorCursor getVectorCursor();

    void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size);

    void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size);

    int getKeySize();

    int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob);
}
