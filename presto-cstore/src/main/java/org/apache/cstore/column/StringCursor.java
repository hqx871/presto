package org.apache.cstore.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;

public class StringCursor
        extends IntCursor
{
    private final Block dictionary;
    private final int sizeInBytes;

    public StringCursor(int[] values, Block dictionary)
    {
        super(values);
        this.dictionary = dictionary;
        this.sizeInBytes = (int) (getCapacity() * Integer.BYTES + dictionary.getSizeInBytes());
    }

    @Override
    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public Block toBlock(int size)
    {
        return new DictionaryBlock(size, dictionary, values);
    }
}
