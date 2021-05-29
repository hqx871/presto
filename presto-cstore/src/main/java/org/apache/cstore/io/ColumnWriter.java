package org.apache.cstore.io;

public interface ColumnWriter<T>
{
    default void open() {}

    int write(T value);

    int flushTo(StreamWriter output);

    void close();
}
