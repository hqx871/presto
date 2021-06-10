package github.cstore.aggregation.cursor;

import github.cstore.aggregation.AggregationCursor;
import github.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

public class AggregationLongCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    public AggregationLongCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putLong(offset, cursor.readLong(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putLong(offset, cursor.readLong(rowOffset + i));
            offset += step;
        }
    }

    @Override
    public int getKeySize()
    {
        return Long.BYTES;
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return Long.compare(a.getLong(oa), b.getLong(ob));
    }
}
