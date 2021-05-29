package org.apache.cstore;

import java.nio.ByteBuffer;

public interface BufferComparator
{
    int compare(ByteBuffer a, int oa, ByteBuffer b, int ob);
}
