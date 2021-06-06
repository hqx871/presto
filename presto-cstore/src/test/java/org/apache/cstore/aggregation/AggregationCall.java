package org.apache.cstore.aggregation;

import java.nio.ByteBuffer;

public interface AggregationCall
{
    void init(ByteBuffer buffer, int offset);

    void add(ByteBuffer buffer, int offset, AggregationCursor cursor, int position);
}

class DoubleSumCall
        implements AggregationCall
{
    @Override
    public void init(ByteBuffer buffer, int offset)
    {
        buffer.putDouble(offset, 0);
    }

    @Override
    public void add(ByteBuffer buffer, int offset, AggregationCursor cursor, int position)
    {
        double sum = buffer.get(offset) + cursor.readDouble(position);
        buffer.putDouble(offset, sum);
    }
}