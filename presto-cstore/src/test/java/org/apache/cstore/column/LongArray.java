package org.apache.cstore.column;

public interface LongArray
{
    void writeLong(long value);

    void writeLongUnchecked(long value);
}
