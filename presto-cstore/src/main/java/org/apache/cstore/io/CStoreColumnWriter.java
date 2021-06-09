package org.apache.cstore.io;

import com.facebook.presto.common.block.Block;

import java.io.IOException;
import java.nio.MappedByteBuffer;

public interface CStoreColumnWriter<T>
{
    default void open() {}

    int write(T value);

    int writeNull();

    T readBlock(Block src, int position);

    int write(Block src, int size);

    int appendTo(StreamWriter output)
            throws IOException;

    void flush()
            throws IOException;

    MappedByteBuffer mapFile();

    void close()
            throws IOException;
}
