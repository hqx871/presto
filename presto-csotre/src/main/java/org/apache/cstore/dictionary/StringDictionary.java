package org.apache.cstore.dictionary;

import org.apache.cstore.BufferComparator;

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
}
