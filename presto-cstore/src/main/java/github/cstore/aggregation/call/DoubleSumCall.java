package github.cstore.aggregation.call;

import github.cstore.aggregation.AggregationCursor;
import github.cstore.column.VectorCursor;

import java.nio.ByteBuffer;
import java.util.List;

public class DoubleSumCall
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
