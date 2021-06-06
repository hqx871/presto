package org.apache.cstore.aggregation;

import org.apache.cstore.column.VectorCursor;

abstract class AbstractAggregationCursor
        implements AggregationCursor
{
    protected final VectorCursor cursor;

    protected AbstractAggregationCursor(VectorCursor cursor)
    {
        this.cursor = cursor;
    }

    @Override
    public VectorCursor getVectorCursor()
    {
        return cursor;
    }
}
