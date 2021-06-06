package org.apache.cstore.aggregation;

import com.facebook.presto.common.block.Block;
import org.apache.cstore.column.ByteCursor;
import org.apache.cstore.column.DoubleCursor;
import org.apache.cstore.column.IntCursor;
import org.apache.cstore.column.LongCursor;
import org.apache.cstore.column.ShortCursor;
import org.apache.cstore.column.StringCursor;
import org.apache.cstore.column.VectorCursor;

import java.nio.ByteBuffer;

public interface AggregationCursor
        extends VectorCursor
{
    void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size);

    void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size);

    int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob);

    default byte readByte(int position)
    {
        throw new UnsupportedOperationException();
    }

    default short readShort(int position)
    {
        throw new UnsupportedOperationException();
    }

    default int readInt(int position)
    {
        throw new UnsupportedOperationException();
    }

    default long readLong(int position)
    {
        throw new UnsupportedOperationException();
    }

    default double readDouble(int position)
    {
        throw new UnsupportedOperationException();
    }
}

class AggregationIntCursor
        extends IntCursor
        implements AggregationCursor
{
    AggregationIntCursor(int[] values)
    {
        super(values);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[rowOffset + i]);
            offset += step;
        }
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
    }

    @Override
    public int readInt(int position)
    {
        return values[position];
    }
}

class AggregationShortCursor
        extends ShortCursor
        implements AggregationCursor
{
    final int[] values;

    AggregationShortCursor(int[] values)
    {
        super(values);
        this.values = values;
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[rowOffset + i]);
            offset += step;
        }
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
    }

    @Override
    public int readInt(int position)
    {
        return values[position];
    }
}

class AggregationByteCursor
        extends ByteCursor
        implements AggregationCursor
{
    final int[] values;

    AggregationByteCursor(int[] values)
    {
        super(values);
        this.values = values;
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[rowOffset + i]);
            offset += step;
        }
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
    }

    @Override
    public int readInt(int position)
    {
        return values[position];
    }
}

class AggregationLongCursor
        extends LongCursor
        implements AggregationCursor
{
    final long[] values;

    AggregationLongCursor(long[] values)
    {
        super(values);
        this.values = values;
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putLong(offset, values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putLong(offset, values[rowOffset + i]);
            offset += step;
        }
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return Long.compare(a.getLong(oa), b.getLong(ob));
    }

    @Override
    public long readLong(int position)
    {
        return values[position];
    }
}

class AggregationDoubleCursor
        extends DoubleCursor
        implements AggregationCursor
{
    final long[] values;

    AggregationDoubleCursor(long[] values)
    {
        super(values);
        this.values = values;
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, values[rowOffset + i]);
            offset += step;
        }
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return Double.compare(a.getDouble(oa), b.getDouble(ob));
    }

    @Override
    public double readDouble(int position)
    {
        return values[position];
    }
}

class AggregationStringCursor
        extends StringCursor
        implements AggregationCursor
{
    final int[] values;

    AggregationStringCursor(int[] values, Block dictionary)
    {
        super(values, dictionary);
        this.values = values;
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, values[rowOffset + i]);
            offset += step;
        }
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
    }

    @Override
    public int readInt(int position)
    {
        return values[position];
    }
}

