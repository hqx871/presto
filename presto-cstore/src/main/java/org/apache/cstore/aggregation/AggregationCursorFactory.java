package org.apache.cstore.aggregation;

import org.apache.cstore.column.ByteCursor;
import org.apache.cstore.column.DoubleCursor;
import org.apache.cstore.column.IntCursor;
import org.apache.cstore.column.LongCursor;
import org.apache.cstore.column.ShortCursor;
import org.apache.cstore.column.StringCursor;
import org.apache.cstore.column.VectorCursor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class AggregationCursorFactory
{
    private final Map<Class<?>, Function<VectorCursor, AggregationCursor>> cursors;

    public AggregationCursorFactory()
    {
        this.cursors = new HashMap<>();
        cursors.put(ByteCursor.class, AggregationByteCursor::new);
        cursors.put(ShortCursor.class, AggregationShortCursor::new);
        cursors.put(IntCursor.class, AggregationIntCursor::new);
        cursors.put(LongCursor.class, AggregationLongCursor::new);
        cursors.put(DoubleCursor.class, AggregationDoubleCursor::new);
        cursors.put(StringCursor.class, AggregationByteCursor::new);
    }

    public AggregationCursor wrap(VectorCursor cursor)
    {
        return cursors.get(cursor.getClass()).apply(cursor);
    }
}
