package org.apache.cstore.column;

@Deprecated
public interface IntVector
{
    int readInt(int position);

    int getRowCount();
}
