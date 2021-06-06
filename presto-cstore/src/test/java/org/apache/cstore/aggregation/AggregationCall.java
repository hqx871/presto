package org.apache.cstore.aggregation;

import java.nio.ByteBuffer;

public interface AggregationCall
{
    int getStateSize();

    void init(ByteBuffer buffer, int offset);

    void add(ByteBuffer buffer, int offset, AggregationCursor cursor, int position);

    void merge(ByteBuffer a, ByteBuffer b, ByteBuffer r);

    void reduce(ByteBuffer row, ByteBuffer out);
}

class DoubleSumCall
        implements AggregationCall
{
    @Override
    public int getStateSize()
    {
        return Double.BYTES;
    }

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

    @Override
    public void merge(ByteBuffer a, ByteBuffer b, ByteBuffer r)
    {
        r.putDouble(a.getDouble() + b.getDouble());
    }

    @Override
    public void reduce(ByteBuffer row, ByteBuffer out)
    {
        out.putDouble(row.getDouble());
    }
}

class DoubleAvgCall
        implements AggregationCall
{
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
    public void add(ByteBuffer buffer, int offset, AggregationCursor cursor, int position)
    {
        long count = buffer.getLong(offset) + 1;
        double sum = buffer.getDouble(offset + Long.BYTES) + cursor.readDouble(position);
        buffer.putLong(offset, count);
        buffer.putDouble(offset + Long.BYTES, sum);
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