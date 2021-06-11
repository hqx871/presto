package github.cstore.column;

import com.facebook.presto.common.block.Block;
import github.cstore.io.StreamWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

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

    ByteBuffer mapFile();

    void close()
            throws IOException;
}
