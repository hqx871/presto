package org.apache.cstore.aggregation;

import org.apache.cstore.BufferComparator;

import java.nio.ByteBuffer;
import java.util.List;

public final class KeyComparator
        implements BufferComparator
{
    private final List<AggregationCursor> keyCursors;
    private final int[] keySizeArray;

    public KeyComparator(List<AggregationCursor> keyCursors, int[] keySizeArray)
    {
        this.keyCursors = keyCursors;
        this.keySizeArray = keySizeArray;
    }

    @Override
    public int compare(ByteBuffer a, int oa, ByteBuffer b, int ob)
    {
        for (int i = 0; i < keyCursors.size(); i++) {
            AggregationCursor keyCursor = keyCursors.get(i);
            int comparision = keyCursor.compareKey(a, oa, b, ob);
            if (comparision != 0) {
                return comparision;
            }
            oa += keySizeArray[i];
            ob += keySizeArray[i];
        }
        return 0;
    }
}
