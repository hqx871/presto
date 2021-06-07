package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;

public final class StringCursor
        implements VectorCursor
{
    private final Block dictionary;
    private final int sizeInBytes;
    private final int[] values;

    public StringCursor(int[] values, Block dictionary)
    {
        this.values = values;
        this.dictionary = dictionary;
        this.sizeInBytes = (int) (getCapacity() * Integer.BYTES + dictionary.getSizeInBytes());
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int getCapacity()
    {
        return values.length;
    }

    @Override
    public Block toBlock(int size)
    {
        return new DictionaryBlock(size, dictionary, values);
    }

    @Override
    public void writeByte(int position, byte value)
    {
        values[position] = value;
    }

    public void writeInt(int position, int value)
    {
        values[position] = value;
    }

    @Override
    public int readInt(int position)
    {
        return values[position];
    }

    @Override
    public long readLong(int position)
    {
        return values[position];
    }

    @Override
    public double readDouble(int position)
    {
        return values[position];
    }
}
