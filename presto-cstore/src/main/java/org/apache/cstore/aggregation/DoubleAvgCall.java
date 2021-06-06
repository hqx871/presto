package org.apache.cstore.aggregation;

import org.apache.cstore.column.VectorCursor;

import java.nio.ByteBuffer;
import java.util.List;

class DoubleAvgCall
        extends UnaryAggregationCall
{
    public DoubleAvgCall(int inputChannel)
    {
        super(inputChannel);
    }

    @Override
    public int getStateSize()
    {
        return Long.BYTES + Double.BYTES;
    }

    @Override
    public void init(ByteBuffer buffer, int offset)
    {
        buffer.putLong(offset, 0);
        buffer.putDouble(offset + Long.BYTES, 0);
    }

    @Override
    public void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int rowOffset, int size)
    {
        VectorCursor cursor = page.get(inputChannel).getVectorCursor();
        for (int i = 0; i < size; i++) {
            int offset = offsets[i + bucketOffset];
            int position = rowOffset + i;
            long count = buffer.getLong(offset) + 1;
            double sum = buffer.getDouble(offset + Long.BYTES) + cursor.readDouble(position);
            buffer.putLong(offset, count);
            buffer.putDouble(offset + Long.BYTES, sum);
        }
    }

    @Override
    public void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int[] positions, int rowOffset, int size)
    {
        VectorCursor cursor = page.get(inputChannel).getVectorCursor();
        for (int i = 0; i < size; i++) {
            int offset = offsets[i + bucketOffset];
            int position = positions[rowOffset + i];
            long count = buffer.getLong(offset) + 1;
            double sum = buffer.getDouble(offset + Long.BYTES) + cursor.readDouble(position);
            buffer.putLong(offset, count);
            buffer.putDouble(offset + Long.BYTES, sum);
        }
    }

    @Override
    public void merge(ByteBuffer a, ByteBuffer b, ByteBuffer r)
    {
        r.putLong(a.getLong() + b.getLong());
        r.putDouble(a.getDouble() + b.getDouble());
    }

    @Override
    public void reduce(ByteBuffer row, ByteBuffer out)
    {
        long count = row.getLong();
        double sum = row.getDouble();
        out.putDouble(sum / count);
    }
}
