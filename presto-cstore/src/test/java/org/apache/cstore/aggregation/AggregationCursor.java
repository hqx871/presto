package org.apache.cstore.aggregation;

import com.facebook.presto.common.block.Block;
import org.apache.cstore.column.ByteCursor;
import org.apache.cstore.column.ConstantDoubleCursor;
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

    int getKeySize();

    int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob);
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
    public int getKeySize()
    {
        return Integer.BYTES;
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
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
            buffer.putShort(offset, (short) values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putShort(offset, (short) values[rowOffset + i]);
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
            buffer.put(offset, (byte) values[positions[i]]);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.put(offset, (byte) values[rowOffset + i]);
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
    public int getKeySize()
    {
        return Long.BYTES;
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return Long.compare(a.getLong(oa), b.getLong(ob));
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

class AggregationConstantDoubleCursor
        extends ConstantDoubleCursor
        implements AggregationCursor
{
    public AggregationConstantDoubleCursor(double doubleValue, int count)
    {
        super(doubleValue, count);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, doubleValue);
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, doubleValue);
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
    public int getKeySize()
    {
        return Integer.BYTES;
    }

    @Override
    public int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        return a.getInt(oa) - b.getInt(ob);
    }
}
