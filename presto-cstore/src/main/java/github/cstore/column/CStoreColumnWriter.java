package github.cstore.column;

import com.facebook.presto.common.block.Block;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CStoreColumnWriter<T>
{
    default void open() {}

    int write(T value);

    int writeNull();

    T readValue(Block src, int position);

    ByteBuffer mapBuffer();

    int getRowCount();

    void close()
            throws IOException;

    void reset();
}
