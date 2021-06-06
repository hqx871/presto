package org.apache.cstore.aggregation;

import java.nio.ByteBuffer;
import java.util.List;

class AggregationReducerImpl
        implements AggregationReducer
{
    private final int keySize;
    private final List<AggregationCall> aggCalls;
    final ByteBuffer state = ByteBuffer.allocate(8 << 10);
    final ByteBuffer result = ByteBuffer.allocate(8 << 10);

    AggregationReducerImpl(int keySize, List<AggregationCall> aggCalls)
    {
        this.keySize = keySize;
        this.aggCalls = aggCalls;
    }

    @Override
    public ByteBuffer merge(ByteBuffer a, ByteBuffer b)
    {
        state.rewind();
        for (int i = 0; i < keySize; i++) {
            state.put(a.get());
        }
        b.position(b.position() + keySize);
        for (AggregationCall aggCall : aggCalls) {
            aggCall.merge(a, b, state);
        }
        state.flip();
        return state;
    }

    @Override
    public ByteBuffer reduce(ByteBuffer row)
    {
        result.rewind();
        for (int i = 0; i < keySize; i++) {
            result.put(row.get());
        }
        for (AggregationCall aggCall : aggCalls) {
            aggCall.reduce(row, result);
        }
        result.flip();
        return result;
    }
}
