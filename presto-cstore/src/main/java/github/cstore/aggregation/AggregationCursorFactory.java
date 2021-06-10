package github.cstore.aggregation;

import github.cstore.aggregation.cursor.AggregationByteCursor;
import github.cstore.aggregation.cursor.AggregationDoubleCursor;
import github.cstore.aggregation.cursor.AggregationIntCursor;
import github.cstore.aggregation.cursor.AggregationLongCursor;
import github.cstore.aggregation.cursor.AggregationShortCursor;
import github.cstore.column.ByteCursor;
import github.cstore.column.DoubleCursor;
import github.cstore.column.IntCursor;
import github.cstore.column.LongCursor;
import github.cstore.column.ShortCursor;
import github.cstore.column.StringCursor;
import github.cstore.column.VectorCursor;

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
