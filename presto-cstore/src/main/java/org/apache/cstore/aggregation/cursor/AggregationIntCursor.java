package org.apache.cstore.aggregation.cursor;

import org.apache.cstore.aggregation.AggregationCursor;
import org.apache.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

public class AggregationIntCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    public AggregationIntCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, cursor.readInt(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, cursor.readInt(rowOffset + i));
            offset += step;
        }
    }

    @Override
    public int getKeySize()
    {
        return Integer.BYTES;
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
    }
}
