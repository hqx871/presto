package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;

public interface VectorCursor
{
    default void writeByte(int position, byte value)
    {
        throw new UnsupportedOperationException();
    }

    default void writeShort(int position, short value)
    {
        throw new UnsupportedOperationException();
    }

    default void writeInt(int position, int value)
    {
        throw new UnsupportedOperationException();
    }

    default void writeLong(int position, long value)
    {
        throw new UnsupportedOperationException();
    }

    default void writeDouble(int position, double value)
    {
        throw new UnsupportedOperationException();
    }

    int getSizeInBytes();

    int getCapacity();

    Block toBlock(int size);
}
