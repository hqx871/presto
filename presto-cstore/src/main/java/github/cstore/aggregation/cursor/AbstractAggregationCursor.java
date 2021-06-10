package github.cstore.aggregation.cursor;

import github.cstore.aggregation.AggregationCursor;
import github.cstore.column.VectorCursor;

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
