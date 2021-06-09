package org.apache.cstore.aggregation;

import org.apache.cstore.sort.BufferComparator;
import org.apache.cstore.util.BufferUtil;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class SortMergeAggregator
{
    private final List<Iterator<ByteBuffer>> inputs;
    private Queue<Iterator<ByteBuffer>> minHeap;
    private final AggregationReducer reducer;
    private final boolean distinct;

    public SortMergeAggregator(List<Iterator<ByteBuffer>> inputs, AggregationReducer reducer, boolean distinct)
    {
        this.inputs = inputs;
        this.reducer = reducer;
        this.distinct = distinct;
    }

    public void setup()
    {
        minHeap = new PriorityQueue<>(inputs);
    }

    public Iterator<ByteBuffer> iterator()
    {
        if (isDistinct()) {
            return distinctIterator();
        }
        else {
            return mergeIterator();
        }
    }

    private Iterator<ByteBuffer> mergeIterator()
    {
        return new Iterator<ByteBuffer>()
        {
            ByteBuffer lastRow;
            BufferComparator comparator;

            @Override
            public boolean hasNext()
            {
                return lastRow != null || minHeap.size() > 0;
            }

            @Override
            public ByteBuffer next()
            {
                while (minHeap.size() > 0) {
                    Iterator<ByteBuffer> rows = minHeap.poll();
                    ByteBuffer row = rows.next();
                    if (rows.hasNext()) {
                        minHeap.add(rows);
                    }
                    if (lastRow == null) {
                        lastRow = row;
                    }
                    else if (lastRow.limit() == row.limit()
                            && BufferUtil.equals(lastRow, 0, row, 0, row.limit())) {
                        lastRow = reducer.merge(lastRow, row);
                    }
                    else {
                        ByteBuffer result = lastRow;
                        lastRow = row;
                        return reducer.reduce(result);
                    }
                }
                ByteBuffer result = lastRow;
                lastRow = null;
                return reducer.reduce(result);
            }
        };
    }

    private Iterator<ByteBuffer> distinctIterator()
    {
        return new Iterator<ByteBuffer>()
        {
            @Override
            public boolean hasNext()
            {
                return minHeap.size() > 0;
            }

            @Override
            public ByteBuffer next()
            {
                Iterator<ByteBuffer> rows = minHeap.poll();
                ByteBuffer row = rows.next();
                if (rows.hasNext()) {
                    minHeap.add(rows);
                }
                return reducer.reduce(row);
            }
        };
    }

    private boolean isDistinct()
    {
        return distinct;
    }

    public void close()
    {
    }
}
