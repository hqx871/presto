package org.apache.cstore.column;

public class LongArrayImpl
        implements LongArray
{
    private final long[] array;
    private int count;

    public LongArrayImpl(int capacity)
    {
        this.array = new long[capacity];
    }

    @Override
    public void writeLong(long value)
    {
        checkPosition(count);
        array[count++] = value;
    }

    @Override
    public void writeLongUnchecked(long value)
    {
        array[count++] = value;
    }

    private void checkPosition(int position)
    {
        if (position >= array.length) {
            throw new RuntimeException("overflow");
        }
    }
}
