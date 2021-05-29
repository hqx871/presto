package org.apache.cstore.io;

public interface CStoreColumnWriter<T>
{
    default void open() {}

    int write(T value);

    int flushTo(StreamWriter output);

    void close();
}
