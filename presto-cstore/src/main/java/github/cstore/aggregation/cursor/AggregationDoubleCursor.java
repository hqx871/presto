package github.cstore.aggregation.cursor;

import github.cstore.aggregation.AggregationCursor;
import github.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

public class AggregationDoubleCursor
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
