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
{
    VectorCursor getVectorCursor();

    void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size);

    void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size);

    int getKeySize();

    int compareKey(ByteBuffer a, int oa, ByteBuffer b, int ob);
}

abstract class AbstractAggregationCursor
        implements AggregationCursor
{
    protected final VectorCursor cursor;

    protected AbstractAggregationCursor(VectorCursor cursor)
    {
        this.cursor = cursor;
    }

    @Override
    public VectorCursor getVectorCursor()
    {
        return cursor;
    }
}

class AggregationIntCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{

    public AggregationIntCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, cursor.readInt(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, cursor.readInt(rowOffset + i));
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
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    //private final int[] values;

    AggregationShortCursor(VectorCursor cursor)
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

class AggregationByteCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    //final int[] values;

    AggregationByteCursor(int[] values)
    {
        super(new ByteCursor(values));
        //this.values = values;
    }

    AggregationByteCursor(VectorCursor cursor)
    {
        super(cursor);
        //this.values = values;
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

class AggregationLongCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{

    public AggregationLongCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putLong(offset, cursor.readLong(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putLong(offset, cursor.readLong(rowOffset + i));
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
        extends AbstractAggregationCursor
        implements AggregationCursor
{

    public AggregationDoubleCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, cursor.readDouble(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putDouble(offset, cursor.readDouble(rowOffset + i));
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

@Deprecated
class AggregationConstantDoubleCursor
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    private final double doubleValue;

    public AggregationConstantDoubleCursor(double doubleValue, int count)
    {
        super(new ConstantDoubleCursor(doubleValue, count));
        this.doubleValue = doubleValue;
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
        extends AbstractAggregationCursor
        implements AggregationCursor
{
    protected AggregationStringCursor(VectorCursor cursor)
    {
        super(cursor);
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int[] positions, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, cursor.readInt(positions[i]));
            offset += step;
        }
    }

    @Override
    public void appendTo(ByteBuffer buffer, int offset, int step, int rowOffset, int size)
    {
        for (int i = 0; i < size; i++) {
            buffer.putInt(offset, cursor.readInt(rowOffset + i));
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
