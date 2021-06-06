package org.apache.cstore.aggregation;

import org.apache.cstore.column.VectorCursor;

import java.util.List;

public interface ScalarCall
{
    void process(List<? extends VectorCursor> page, int size);

    void process(List<? extends VectorCursor> page, int[] positions, int size);
}
