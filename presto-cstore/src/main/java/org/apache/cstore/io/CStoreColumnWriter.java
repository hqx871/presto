package org.apache.cstore.io;

import java.io.IOException;

public interface CStoreColumnWriter<T>
{
    default void open() {}

    int write(T value);

    int flushTo(StreamWriter output)
            throws IOException;

    void flush()
            throws IOException;

    void close()
            throws IOException;
}
