package org.apache.cstore.aggregation;

import org.apache.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

class AggregationDoubleCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    public AggregationDoubleCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, cursor.readDouble(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, cursor.readDouble(rowOffset + i));
            offset += step;
        }
    }

    @Override
    public int getKeySize()
    {
        return Double.BYTES;
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return Double.compare(a.getDouble(oa), b.getDouble(ob));
    }
}
