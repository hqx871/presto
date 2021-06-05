package org.apache.cstore.io;

import java.io.IOException;
import java.nio.MappedByteBuffer;

public interface CStoreColumnWriter<T>
{
    default void open() {}

    int write(T value);

    int appendTo(StreamWriter output)
            throws IOException;

    void flush()
            throws IOException;

    MappedByteBuffer mapFile();

    void close()
            throws IOException;
}
