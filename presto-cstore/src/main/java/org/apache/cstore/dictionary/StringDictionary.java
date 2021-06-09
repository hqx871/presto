package org.apache.cstore.dictionary;

import com.facebook.presto.common.block.Block;
import org.apache.cstore.sort.BufferComparator;

public abstract class StringDictionary
        implements Dictionary<String>
{
    protected static final int INVALID_ID = -1;

    public final byte getNullValueId()
    {
        return 0;
    }

    protected final byte getNonNullValueStartId()
    {
        return 1;
    }

    public abstract BufferComparator encodeComparator();

    public abstract boolean isSort();

    public Block getDictionaryValue()
    {
        throw new UnsupportedOperationException();
    }
}
