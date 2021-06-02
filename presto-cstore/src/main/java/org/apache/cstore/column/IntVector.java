package org.apache.cstore.column;

public interface IntVector
{
    int readInt(int position);

    int getRowCount();
}
