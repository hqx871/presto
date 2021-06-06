package org.apache.cstore.aggregation.cursor;

import org.apache.cstore.aggregation.AggregationCursor;
import org.apache.cstore.column.VectorCursor;

public abstract class AbstractAggregationCursor
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
