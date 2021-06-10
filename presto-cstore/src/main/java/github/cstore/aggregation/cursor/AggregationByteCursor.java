package github.cstore.aggregation.cursor;

import github.cstore.aggregation.AggregationCursor;
import github.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

public class AggregationByteCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    public AggregationByteCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.put(offset, cursor.readByte(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.put(offset, cursor.readByte(rowOffset + i));
            offset += step;
        }
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
    }

    @Override
    public int getKeySize()
    {
        return Byte.BYTES;
    }
}
