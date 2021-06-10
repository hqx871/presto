package github.cstore.sort;

import java.nio.ByteBuffer;

public interface BufferComparator
{
    int compare(ByteBuffer a, int oa, ByteBuffer b, int ob);
}
