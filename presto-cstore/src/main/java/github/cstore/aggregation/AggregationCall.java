package github.cstore.aggregation;

import java.nio.ByteBuffer;
import java.util.List;

public interface AggregationCall
{
    int getStateSize();

    void init(ByteBuffer buffer, int offset);

    void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int rowOffset, int size);

    void add(ByteBuffer buffer, int[] offsets, int bucketOffset, List<AggregationCursor> page, int[] positions, int rowOffset, int size);

    void merge(ByteBuffer a, ByteBuffer b, ByteBuffer r);

    void reduce(ByteBuffer row, ByteBuffer out);
}
