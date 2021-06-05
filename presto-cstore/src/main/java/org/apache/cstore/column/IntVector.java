package org.apache.cstore.column;

public interface IntVector
{
    @Deprecated
    int readInt(int position);

    int getRowCount();
}
