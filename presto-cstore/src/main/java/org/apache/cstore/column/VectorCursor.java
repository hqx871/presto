package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;

public interface VectorCursor
{
    default void writeBoolean(int position, boolean value)
    {
        throw new UnsupportedOperationException();
    }

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
