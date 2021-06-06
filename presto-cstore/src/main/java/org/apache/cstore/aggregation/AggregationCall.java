package org.apache.cstore.aggregation;

import org.apache.cstore.column.VectorCursor;

import java.nio.ByteBuffer;
import java.util.List;

public interface AggregationCall
{
    int getStateSize();

    void init(ByteBuffer buffer, int offset);

    void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int rowOffset, int size);

    void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int[] positions, int rowOffset, int size);

    void merge(ByteBuffer a, ByteBuffer b, ByteBuffer r);

    void reduce(ByteBuffer row, ByteBuffer out);
}

abstract class UnaryAggregationCall
        implements AggregationCall
{
    protected final int inputChannel;

    protected UnaryAggregationCall(int inputChannel)
    {
        this.inputChannel = inputChannel;
    }
}

class DoubleSumCall
        extends UnaryAggregationCall
{
    public DoubleSumCall(int inputChannel)
    {
        super(inputChannel);
    }

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
    public void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int rowOffset, int size)
    {
        VectorCursor cursor = page.get(inputChannel).getVectorCursor();
        for (int i = 0; i < size; i++) {
            int offset = offsets[i + bucketOffset];
            int position = rowOffset + i;
            double sum = buffer.get(offset) + cursor.readDouble(position);
            buffer.putDouble(offset, sum);
        }
    }

    @Override
    public void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int[] positions, int rowOffset, int size)
    {
        VectorCursor cursor = page.get(inputChannel).getVectorCursor();
        for (int i = 0; i < size; i++) {
            int offset = offsets[i + bucketOffset];
            int position = positions[rowOffset + i];
            double sum = buffer.get(offset) + cursor.readDouble(position);
            buffer.putDouble(offset, sum);
        }
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

class CountStarCall
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