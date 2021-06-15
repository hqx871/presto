package github.cstore.column;

import java.util.Optional;

public abstract class AbstractVectorCursor
        implements VectorCursor
{
    protected final boolean[] nullMask;
    protected int nullValueCount;
    protected final int capacity;

    protected AbstractVectorCursor(int capacity)
    {
        this.capacity = capacity;
        this.nullMask = new boolean[capacity];
    }

    @Override
    public void clear()
    {
        nullValueCount = 0;
    }

    @Override
    public int getNullValueCount()
    {
        return nullValueCount;
    }

    @Override
    public final void setNull(int position)
    {
        nullMask[position] = true;
        nullValueCount++;
    }

    @Override
    public final boolean isNull(int position)
    {
        return nullMask[position];
    }

    @Override
    public final int getCapacity()
    {
        return capacity;
    }

    protected final Optional<boolean[]> getNullMask()
    {
        if (nullValueCount > 0) {
            return Optional.of(nullMask);
        }
        else {
            return Optional.empty();
        }
    }

    protected final int getMaskSizeInBytes()
    {
        return nullValueCount > 0 ? Byte.BYTES * capacity : 0;
    }
}
