package org.apache.cstore.aggregation.call;

import org.apache.cstore.aggregation.AggregationCall;
import org.apache.cstore.aggregation.AggregationCursor;

import java.nio.ByteBuffer;
import java.util.List;

public class CountStarCall
        implements AggregationCall
{
    @Override
    public int getStateSize()
    {
        return Long.BYTES;
    }

    @Override
    public void init(ByteBuffer buffer, int offset)
    {
        buffer.putLong(offset, 0);
    }

    @Override
    public void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            int offset = offsets[i + bucketOffset];
            long count = buffer.getLong(offset) + 1;
            buffer.putLong(offset, count);
        }
    }

    @Override
    public void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int[] positions, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            int offset = offsets[i + bucketOffset];
            long count = buffer.getLong(offset) + 1;
            buffer.putLong(offset, count);
        }
    }

    @Override
    public void merge(ByteBuffer a, ByteBuffer b, ByteBuffer r)
    {
        long count = a.getLong() + b.getLong();
        r.putLong(count);
    }

    @Override
    public void reduce(ByteBuffer row, ByteBuffer out)
    {
        long count = row.getLong();
        out.putLong(count);
    }
}
