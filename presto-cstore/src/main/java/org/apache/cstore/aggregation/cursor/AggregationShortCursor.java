package org.apache.cstore.aggregation.cursor;

import org.apache.cstore.aggregation.AggregationCursor;
import org.apache.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

public class AggregationShortCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    //private final int[] values;

    public AggregationShortCursor(VectorCursor cursor)
    {
        super(cursor);
        //this.values = values;
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putShort(offset, cursor.readShort(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putShort(offset, cursor.readShort(rowOffset + i));
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
        return Short.BYTES;
    }
}
