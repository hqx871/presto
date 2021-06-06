package org.apache.cstore.aggregation;

import java.nio.ByteBuffer;

public interface AggregationReducer
{
    ByteBuffer merge(ByteBuffer a, ByteBuffer b);

    ByteBuffer reduce(ByteBuffer row);
}
